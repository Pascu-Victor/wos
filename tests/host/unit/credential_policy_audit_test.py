#!/usr/bin/env python3
"""Exercise the source gate with new, duplicated, removed and disguised checks."""

import importlib.util
import json
import re
from pathlib import Path
import tempfile


ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("credential_policy_audit", ROOT / "scripts/test/credential_policy_audit.py")
AUDIT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(AUDIT)


def expect_error(callback, fragment):
    try:
        callback()
    except ValueError as error:
        assert fragment in str(error), error
    else:
        raise AssertionError(f"expected {fragment}")


def synthetic_manifest(source, status="enforced"):
    return {
        "schema_version": 1,
        "operations": [
            {"id": name, "status": status, "decision": "deny without authority", "sources": [source]}
            for name in sorted(AUDIT.REQUIRED_OPERATIONS)
        ],
        "legacy_root_checks": [],
    }


def test_gate_requires_every_domain_even_when_remaining_rows_are_enforced():
    with tempfile.TemporaryDirectory(prefix="wos-credential-inventory-") as temporary:
        root = Path(temporary)
        relative = "modules/kern/src/sample.cpp"
        source = root / relative
        source.parent.mkdir(parents=True)
        source.write_text("")
        complete = synthetic_manifest(relative)
        assert AUDIT.audit(root, complete, require_complete=True) == 0
        for missing in AUDIT.REQUIRED_OPERATIONS:
            partial = synthetic_manifest(relative)
            partial["operations"] = [row for row in partial["operations"] if row["id"] != missing]
            for require_complete in (False, True):
                expect_error(lambda: AUDIT.audit(root, partial, require_complete=require_complete),
                             f"missing required operations: {missing}")
        malformed = synthetic_manifest(relative)
        malformed["operations"].append(None)
        expect_error(lambda: AUDIT.audit(root, malformed), "invalid operation contract")
        duplicate = synthetic_manifest(relative)
        duplicate["operations"].append(duplicate["operations"][0])
        expect_error(lambda: AUDIT.audit(root, duplicate), "duplicate operation ID")


def test_lexer_ignores_comments_and_literals_but_detects_root_spellings():
    assert not AUDIT.root_checks('// task->euid == 0\n/* !x.euid */ "p.euid != 0" R"tag(p.euid==0)tag"')
    for source in ["p->euid == 0", "p.euid != 0U", "0x00UL == p->euid", "!(p->euid)", "p->euid > 0", "0 >= p.euid"]:
        assert sum(AUDIT.root_checks(source).values()) == 1, source
    assert not AUDIT.root_checks("p.sender_euid != 0")
    assert sum(AUDIT.root_checks("int a = 1'000; bool b = p.euid == 0; int c = 2'000;").values()) == 1
    for source in ["task->credentials().user().effective == 0", "0 == task->credentials().user().effective", "ids.effective != 0"]:
        assert sum(AUDIT.root_checks(source).values()) == 1, source


def test_gate_rejects_equivalent_integral_zero_checks():
    # Exercise the public inventory audit, not only its expression matcher.
    expressions = [
        "p.euid == 0b0", "0B0u != p.euid", "p.euid == 0'0", "0x0'0UL == p.euid",
        "p.euid == static_cast<unsigned>(0)", "static_cast<uid_t>(0) != p.euid",
        "p.euid == uid_t{0}", "uid_t(0) == p.euid", "p.euid == (uid_t)0",
        "p.euid not_eq 0", "not p.euid", "not (p.euid)", "p.effective_uid() == 0",
        "p.euid == 0uz", "p.euid == false",
    ]
    with tempfile.TemporaryDirectory(prefix="wos-credential-zero-") as temporary:
        root = Path(temporary)
        relative = "modules/kern/src/sample.cpp"
        source = root / relative
        source.parent.mkdir(parents=True)
        manifest = synthetic_manifest(relative)
        for expression in expressions:
            source.write_text(f"bool privileged = {expression};\n")
            expect_error(lambda: AUDIT.audit(root, manifest, require_complete=True), "raw root-check drift")
        for expression in expressions:
            source.write_text('// ' + expression + '\nconst char* note = "' + expression + '";\n')
            assert AUDIT.audit(root, manifest, require_complete=True) == 0
    # A zero digit inside another numeric token is not the integer zero.
    for value in ["0.5", "0.0", "0e0", "0x0p0", "10", "0x10", "0b10", "0'1", "1'0", "10U"]:
        for expression in [f"p.euid == {value}", f"{value} != p.euid"]:
            assert not AUDIT.root_checks(expression), expression


def test_gate_rejects_new_duplicate_and_removed_exceptions():
    with tempfile.TemporaryDirectory(prefix="wos-credential-audit-") as temporary:
        root = Path(temporary)
        relative = "modules/kern/src/sample.cpp"
        source = root / relative
        source.parent.mkdir(parents=True)
        code = "bool privileged = task->euid == 0;\n"
        source.write_text(code)
        manifest = synthetic_manifest(relative, "pending")
        manifest["legacy_root_checks"] = [
            {"path": relative, "operation": "credentials", "reason": "pending migration", "checks": {"task->euid==0": 1}}
        ]
        assert AUDIT.audit(root, manifest) == 1
        source.write_text(code + code)
        expect_error(lambda: AUDIT.audit(root, manifest), "drift")
        source.write_text("")
        expect_error(lambda: AUDIT.audit(root, manifest), "drift")
        source.write_text(code)
        expect_error(lambda: AUDIT.audit(root, manifest, require_complete=True), "still pending")
        for operation in manifest["operations"]:
            operation["status"] = "enforced"
        expect_error(lambda: AUDIT.audit(root, manifest, require_complete=True), "legacy root checks remain")
        manifest["legacy_root_checks"] = []
        source.write_text("")
        assert AUDIT.audit(root, manifest, require_complete=True) == 0


def test_real_tree_matches_manifest():
    manifest = json.loads((ROOT / AUDIT.MANIFEST).read_text(), object_pairs_hook=AUDIT.unique_object)
    AUDIT.audit(ROOT, manifest)


def test_duplicate_keys_are_rejected():
    expect_error(lambda: json.loads('{"schema_version":1,"schema_version":2}', object_pairs_hook=AUDIT.unique_object), "duplicate")


def body_after(source, marker):
    position = source.index(marker)
    while True:
        start = source.index("{", position)
        semicolon = source.find(";", position, start)
        if semicolon < 0:
            break
        position = source.index(marker, position + len(marker))
    depth = 0
    for index in range(start, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[start:index + 1]
    raise AssertionError(f"unterminated {marker}")


def test_control_authorization_precedes_side_effects():
    process = (ROOT / "modules/kern/src/syscalls_impl/process/process.cpp").read_text()
    kill = body_after(process, "auto wos_proc_kill(")
    assert kill.count("self->credentials()") == 1
    positive = body_after(process, "auto signal_as(")
    deliver = body_after(process, "auto signal_local_as(")
    assert deliver.count("admission.admit_local(target)") == 1
    assert "PendingSignalTarget retained(target)" in positive
    assert "wki_proxy_task_signal_as(target, caller, caller_job, signum)" in positive
    assert "remote.has_value()) return *remote" in positive
    assert "wki_proxy_task_forward_signal" not in positive + deliver
    assert "local.has_value()) return *local" in positive
    assert positive.count("wki_proxy_task_signal_as(target, caller, caller_job, signum)") == 2
    assert "signum).value_or(-ESRCH)" in positive
    check = deliver.index("security::may_signal(")
    assert deliver.index("SharedVmemPublicationGuard") < deliver.index("same_group(*group, JOB)") < check
    for effect in ["if (signum == 0)", "report_signal_stop", "signal_generate_pending_mask", "wake_task_for_signal"]:
        assert check < deliver.index(effect), effect
    assert "target.type != sched::task::TaskType::PROCESS" in deliver
    assert "sched::session::same_session(caller_job, JOB)" in deliver
    assert "signal_as(*target, CALLER, CALLER_JOB, sig)" in kill
    assert "wki_group_signal_as(CALLER, CALLER_JOB, PGRP, pid >= -1, sig)" in kill
    assert "get_active_task_at_safe" not in kill
    group = body_after(process, "auto signal_group_local_as(")
    assert group.index("cohort.capture(") < group.index("cohort.take()") < group.index("signal_local_as(")
    assert "PendingSignalTarget retained(*target)" in group
    assert "if (CAPTURED != 0) return" in group
    assert "if (ADMISSION.remote)" in deliver, "receiver groups must exclude accepted and parked source representatives"
    assert "ADMISSION.credentials" in deliver
    assert "sched::signal_process_group(" not in kill + group
    assert "RESULT != 0 && RESULT != -ESRCH && (error == -ESRCH || RESULT == -EPERM)" in group
    assert "accepted != 0 ? 0 : error" in group
    priority = body_after(process, "auto wos_proc_setpriority(")
    guard = priority.index("SharedVmemPublicationGuard publication_guard")
    check = priority.index("security::authorize_priority(")
    commit = priority.index("mod::sched::set_task_nice(")
    assert guard < priority.index("self->credentials()") < check < commit
    assert guard < priority.index("target->credentials()") < check
    assert check < priority.index("if (result == 0)") < commit
    assert "target->pid != self->pid" not in priority
    getpriority = body_after(process, "auto wos_proc_getpriority(")
    assert getpriority.index("SharedVmemPublicationGuard publication_guard") < getpriority.index("target->sched_nice")
    dispatch = body_after(process, "case abi::process::procmgmt_ops::SETHOSTNAME:")
    assert "set_hostname_for_subject(ker::mod::sched::get_current_task(), a2, static_cast<size_t>(a3))" in dispatch
    hostname = body_after(process, "auto set_hostname_for_subject(")
    check = hostname.index("authorize_hostname_change(CREDENTIALS)")
    assert hostname.index("host_namespace_admission(task)") < check
    assert check < hostname.index("copy_from_task") < hostname.index("hostname::set")
    syscall = (ROOT / "modules/kern/src/platform/sys/syscall.cpp").read_text()
    dispatch = body_after(syscall, "case abi::callnums::DEBUG:")
    assert "debug_interrupt_control_for_subject(sched::get_current_task(), A1)" in dispatch
    debug = body_after(syscall, "auto debug_interrupt_control_for_subject(")
    check = debug.index("Authority::KERNEL_DEBUG")
    assert debug.index("host_namespace_admission(task)") < check
    assert check < debug.index('asm volatile("cli"')
    assert check < debug.index('asm volatile("sti"')
    power = (ROOT / "modules/kern/src/platform/power/power.cpp").read_text()
    power_authority = body_after(power, "auto authorize_current_power(")
    assert power_authority.index("host_namespace_admission(task)") < power_authority.index("Authority::POWER")
    for name in ["auto prepare_shutdown(", "auto begin_reboot_command("]:
        operation = body_after(power, name)
        assert operation.index("authorize_current_power(subject)") < operation.index("g_phase.compare_exchange_weak")
    assert "return prepare_shutdown(sched::get_current_task());" in power
    assert "return begin_reboot_command(nullptr, cmd, true);" in power
    reboot = body_after(power, "auto begin_reboot_command(")
    assert reboot.index("return PARSE_RET;") < reboot.index("sched::get_current_task()") < reboot.index("authorize_current_power(subject)")
    assert "return begin_reboot_command(subject, cmd, false);" in power


def test_ptrace_and_dumpability_use_shared_policy():
    ptrace = (ROOT / "modules/kern/src/platform/debug/ptrace.cpp").read_text()
    attach = body_after(ptrace, "auto attach(")
    assert attach.index("SharedVmemPublicationGuard publication_guard") < attach.index("can_trace(") < attach.index("attach_tracer(")
    require = body_after(ptrace, "auto require_traced(")
    assert "!can_trace(tracer, target, admitted_target)" in require
    permission = body_after(ptrace, "auto can_trace(")
    assert permission.count("tracer.credentials()") == 1 and permission.count("target.credentials()") == 1
    assert "security::may_trace(TRACER, TARGET)" in permission
    assert permission.index("security::may_trace(TRACER, TARGET)") < permission.index("*admitted_target = TARGET")
    preparation = body_after(ptrace, "auto prepare_target_memory(")
    admission = body_after(preparation, "if (operation != nullptr && operation->private_scope() != nullptr)")
    assert admission.index("SharedVmemPublicationGuard") < admission.index("effect.begin(target)")
    assert preparation.index("effect.begin(target)") < preparation.index("pin_task_user_page(")
    # The sleeping image guard ends before a fault can allocate or wait.
    assert "pin_task_user_page(" not in admission
    assert "writable, true, pin" in preparation
    assert preparation.index("target.credentials() != admitted_target") < preparation.index("pin_task_user_page(")
    for name in ("copy_prepared_task_memory", "write_prepared_task_word"):
        operation = body_after(ptrace, "auto " + name + "(")
        assert operation.index("SharedVmemPublicationGuard publication_guard") < operation.index("target.credentials() != admitted_target")
        assert operation.index("target.credentials() != admitted_target") < operation.index("pin_task_user_page(") < operation.index("std::memcpy(")
        assert "true, true," not in operation and "write_target, true," not in operation
    bulk = body_after(ptrace, "auto transfer_memory(")
    refusal = body_after(bulk, "if (result == as_error(EPERM)")
    assert "if (done == 0) return result" in refusal and "result = as_error(EFAULT)" in refusal
    assert bulk.index("if (result == as_error(EPERM)") < bulk.index("io.transferred = done")
    for request in ("PEEKDATA", "POKEDATA", "READ_MEM", "WRITE_MEM"):
        dispatch = body_after(ptrace, "case abi::ptrace::request::" + request + ":")
        assert "require_traced(*tracer, *target, &admitted_target)" in dispatch
        assert dispatch.count("admitted_target") == 3
    capture = body_after(ptrace, "auto snapshot_target_images(")
    assert capture.index("SharedVmemPublicationGuard publication_guard") < capture.index("target.credentials() != admitted_target")
    assert capture.index("target.credentials() != admitted_target") < capture.index("SnapshotSource const SOURCE") < capture.index("snapshot_images(")
    for name, request in (("list_images", "GET_IMAGES"), ("list_image_catalog", "GET_IMAGE_CATALOG")):
        operation = body_after(ptrace, "auto " + name + "(")
        refusal = body_after(operation, "if (!snapshot_target_images(")
        assert "admitted_target, *runtime_images, needed, status" in operation
        assert "return failure" in refusal and "&failure" in operation
        assert "OwnedBuffer<" in operation and "runtime_storage(operation" in operation
        assert operation.index("snapshot_target_images(") < operation.index("list.count = needed")
        dispatch = body_after(ptrace, "case abi::ptrace::request::" + request + ":")
        assert "require_traced(*tracer, *target, &admitted_target)" in dispatch
        assert name + "(*tracer, *target, data, admitted_target, &operation)" in dispatch
    detach = body_after(ptrace, "auto detach(")
    assert "target.ptrace_tracer_pid != tracer.pid" in detach and "require_traced(" not in detach
    wait = body_after(ptrace, "auto syscall_wait(")
    loop = wait[wait.index("for (;;)"):]
    assert loop.index("capture_wait_event(") < loop.index("publish_wait_event(") < loop.index("preemptible_syscall_park(")
    begin = body_after(ptrace, "auto begin_syscall_wait(")
    assert begin.index("SharedVmemPublicationGuard") < begin.index("target.credentials() != admitted_target") < begin.index("resume_syscall_after_require(")
    capture_wait = body_after(ptrace, "auto capture_wait_event(")
    assert capture_wait.index("SharedVmemPublicationGuard") < capture_wait.index("require_traced(") < capture_wait.index("claim_or_register(") < capture_wait.index("fill_stop_info(")
    publish = body_after(ptrace, "auto publish_wait_event(")
    assert publish.index("copy_value_to_task(") < publish.index("commit_claim(")
    assert "SharedVmemPublicationGuard" not in wait and "SharedVmemPublicationGuard" not in publish
    for name in ("capture_threads", "capture_remote_info", "begin_syscall_wait", "capture_wait_event"):
        helper = body_after(ptrace, "auto " + name + "(")
        assert "SharedVmemPublicationGuard" in helper and "target.credentials() !=" in helper
        assert "usercopy::" not in helper and "preemptible_syscall_park" not in helper
    threads = body_after(ptrace, "auto list_threads(")
    assert threads.index("new (std::nothrow)") < threads.index("capture_threads(target, admitted_target") < threads.index("copy_to_task(")
    dispatch = body_after(ptrace, "case abi::ptrace::request::LIST_THREADS:")
    assert "require_traced(*tracer, *target, &admitted_target)" in dispatch
    assert "list_threads(*tracer, *target, data, admitted_target, &operation)" in dispatch
    event = body_after(ptrace, "case abi::ptrace::request::GETEVENTMSG:")
    assert event.index("SharedVmemPublicationGuard") < event.index("require_traced(") < event.index("fill_event(") < event.index("copy_value_to_task(")
    remote = body_after(ptrace, "auto remote_info(")
    assert remote.index("can_trace(") < remote.index("capture_remote_info(") < remote.index("wki_submitted_task_remote_info(") < remote.index("copy_value_to_task")
    assert "wki_proxy_task_remote_info(" not in remote and "SharedVmemPublicationGuard" not in remote
    process = (ROOT / "modules/kern/src/syscalls_impl/process/process.cpp").read_text()
    option = body_after(process, "case WOS_PR_SET_PTRACER:")
    assert "ptrace_tracer_pid =" not in option
    assert "Change::SET_DUMPABLE" in process
    task = (ROOT / "modules/kern/src/platform/sched/task.hpp").read_text()
    assert "int32_t dumpable" not in task
    core = body_after((ROOT / "modules/kern/src/platform/dbg/coredump.cpp").read_text(), "void try_write_for_task(")
    assert core.count("task->credentials()") == 1
    assert core.index("CREDENTIALS.dumpable()") < core.index("admit_dump(") < core.index("CoreDumpRequest& req")


def test_ptrace_register_validation_precedes_mutation():
    ptrace = (ROOT / "modules/kern/src/platform/debug/ptrace.cpp").read_text()
    gprs = body_after(ptrace, "auto set_gprs(")
    assert gprs.index("valid_debug_gprs(") < gprs.index("stopped_user_frame(") < gprs.index("regs.rax =")
    read = body_after(ptrace, "auto fill_gprs(")
    assert read.index("stopped_user_frame(") < read.index("out.rax =")
    xstate = body_after(ptrace, "auto apply_xstate(")
    assert xstate.index("valid_debug_xstate(") < xstate.index("current_cpu_for_task(") < xstate.index("std::memcpy(target.fx_state")
    assert "supported_mxcsr_mask()" in xstate
    hardware = body_after(ptrace, "auto apply_hw_break(")
    assert hardware.index("valid_debug_breakpoint(") < hardware.index("stopped_user_frame(") < hardware.index("target.ptrace_dr_addr.at")
    for name in ("capture_gprs", "apply_gprs", "capture_xstate", "apply_xstate", "apply_hw_break"):
        operation = body_after(ptrace, "auto " + name + "(")
        assert operation.index("SharedVmemPublicationGuard publication_guard") < operation.index("target.credentials() != admitted_target")
        assert "usercopy::" not in operation
    read = body_after(ptrace, "auto read_regset(")
    assert "capture_gprs(target, out, admitted_target, operation)" in read and "capture_xstate(target, out, admitted_target, operation)" in read
    assert "target.fx_state" not in read
    write = body_after(ptrace, "auto write_regset(")
    assert "apply_gprs(target, in, admitted_target, operation)" in write and "apply_xstate(target, state, admitted_target, operation)" in write
    hardware_input = body_after(ptrace, "auto set_hw_break(")
    assert hardware_input.index("copy_value_from_task(") < hardware_input.index("apply_hw_break(target, desc, enable, admitted_target, operation)")
    for request in ("GETREGSET", "SETREGSET", "SET_HW_BREAK", "DEL_HW_BREAK"):
        dispatch = body_after(ptrace, "case abi::ptrace::request::" + request + ":")
        assert "require_traced(*tracer, *target, &admitted_target)" in dispatch and dispatch.count("admitted_target") == 3
    options = body_after(ptrace, "auto set_trace_options(")
    assert options.index("SharedVmemPublicationGuard") < options.index("require_traced(") < options.index("target.ptrace_options =")
    assert "set_trace_options(*tracer, *target, data, &operation" in ptrace
    for request in ("KILL", "CONT", "SINGLESTEP", "SYSCALL", "INTERRUPT"):
        dispatch = body_after(ptrace, "case abi::ptrace::request::" + request + ":")
        assert dispatch.index("SharedVmemPublicationGuard publication_guard") < dispatch.index("require_traced(")
        assert "usercopy::" not in dispatch
    detach = body_after(ptrace, "auto detach(")
    assert detach.index("SharedVmemPublicationGuard publication_guard") < detach.index("target.ptrace_tracer_pid != tracer.pid")
    snapshot = body_after(ptrace, "auto capture_async_syscall_return_snapshot(")
    assert snapshot.index("current_cpu_for_task(") < snapshot.index("live_syscall_saved_regs(") < snapshot.index("target.ptrace_syscall_regs =")
    assert "target.ptrace_syscall_regs = target.context.regs" not in snapshot


def test_shm_authorization_precedes_pins_and_output():
    source = (ROOT / "modules/kern/src/syscalls_impl/shm/shm.cpp").read_text()
    for name in ["shmget_impl", "shmat_impl", "shmctl_impl"]:
        body = body_after(source, "auto " + name + "(")
        assert body.count("task->credentials()") == 1
        assert body.index("task->credentials()") < body.index("g_lock.lock_irqsave()")
    attach = body_after(source, "auto shmat_impl(")
    check = attach.index("may_access_ipc(")
    for effect in ["find_free_attachment()", "segment->attach_count++", "page_ref_inc(", "map_page("]:
        assert check < attach.index(effect)
    control = body_after(source, "auto shmctl_impl(")
    assert control.index("may_control_ipc(") < control.index("segment->marked_destroy = true")
    stat = body_after(control, "case ker::abi::shm::IPC_STAT:")
    assert stat.index("may_access_ipc(") < stat.index("fill_stat(") < stat.index("copy_value_to_task(")
    creation = body_after(source, "auto create_segment(")
    assert ".creator_uid = credentials.user().effective" in creation
    assert ".creator_gid = credentials.group().effective" in creation
    assert "& 0777U" in creation


def test_shm_write_limit_survives_mapping_lifetime():
    shm = (ROOT / "modules/kern/src/syscalls_impl/shm/shm.cpp").read_text()
    attach = body_after(shm, "auto shmat_impl(")
    assert "page_flags |= ker::mod::mm::paging::PAGE_WRITE_RESTRICTED" in attach
    vmem = (ROOT / "modules/kern/src/syscalls_impl/vmem/sys_vmem.cpp").read_text()
    protect = body_after(vmem, "case ker::abi::vmem::ops::PROTECT:")
    assert protect.index("SharedVmemPublicationGuard publication_guard") < protect.index("is_page_write_restricted(")
    assert protect.index("is_page_write_restricted(") < protect.index("protect_shared_vmem_range_locked(") < protect.index("unify_page_flags(")
    assert "-EACCES" in protect
    virt = (ROOT / "modules/kern/src/platform/mm/virt.opt.cpp").read_text()
    update = body_after(virt, "void unify_page_flags(")
    assert "!paging::shared_write_restricted(OLD_RAW)" in update
    query = body_after(virt, "auto is_page_write_restricted(")
    assert "present_mapping_entry(page_table, vaddr)" in query
    assert "user_mapping_pin_lock.lock_irqsave()" in query
    assert "raw = pte_raw(pte);\n        if (paging::shared_write_restricted(raw))" in virt


def test_tmpfs_metadata_authorizes_locked_object_before_commit():
    tmpfs = (ROOT / "modules/kern/src/vfs/fs/tmpfs.cpp").read_text()
    for operation in ["chmod", "chown"]:
        prepare = body_after(tmpfs, f"auto {operation}_locked(")
        assert prepare.index(f"prepare_file_{operation}(") < prepare.index("if (RET == 0)") < prepare.index("commit_metadata_locked(")
        for surface in ["lookup", "node"]:
            body = body_after(tmpfs, f"auto tmpfs_{operation}_{surface}(")
            assert body.index("MutexGuard guard(tmpfs_lock)") < body.index(f"{operation}_locked(")
            assert "credentials" in body
        lookup = body_after(tmpfs, f"auto tmpfs_{operation}_lookup(")
        assert lookup.index("tmpfs_lookup_target_locked(") < lookup.index(f"{operation}_locked(")
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    for operation in ["chmod", "chown"]:
        fd = body_after(core, f"auto vfs_f{operation}_for_task(")
        assert fd.count("task->credentials()") == 1
        assert fd.index("task->credentials()") < fd.index("vfs_get_file_retain(") < fd.index(f"vfs_f{operation}_file_as(")
        fd = body_after(core, f"auto vfs_f{operation}_file_as(")
        assert fd.index("validate_retained_metadata_file(") < fd.index("MutexGuard publication_guard(")
        assert fd.index(f"tmpfs_{operation}_node(") < fd.index("if (result == 0)") < fd.index("cache_notify_file_metadata_changed_impl(")
        path = body_after(core, f"auto vfs_f{operation}at(")
        if operation == "chown": path += body_after(core, "auto consume_chown_lookup(")
        assert "handle.authorization().credentials" in path
    assert "handle.authorization().credentials" in body_after(core, "auto vfs_chmod_resolved_beneath(")


def test_xfs_metadata_checks_before_transaction_and_preserves_lookup_identity():
    xfs = (ROOT / "modules/kern/src/vfs/fs/xfs/xfs_vfs.cpp").read_text()
    for operation in ["chmod", "chown"]:
        policy = body_after(xfs, f"auto {operation}_inode_locked(")
        assert policy.index(f"prepare_file_{operation}(") < policy.index("RET != 0 ? RET : commit_file_identity_locked(")
        assert "credentials" in policy
        assert "get_current_task" not in policy
    commit = body_after(xfs, "auto commit_file_identity_locked(")
    assert commit.index("xfs_trans_capture_inode(") < commit.index("ip->mode =") < commit.index("xfs_trans_commit(")
    assert commit.index("xfs_trans_commit(") < commit.index("if (RET == 0 && statbuf != nullptr)") < commit.index("fill_stat(")
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    for operation in ["chmod", "chown"]:
        path = body_after(core, f"auto vfs_f{operation}at(")
        if operation == "chown": path += body_after(core, "auto consume_chown_lookup(")
        start = path.index(f"xfs_{operation}_lookup(")
        assert "handle.authorization().credentials" in path[start:path.index(";", start)]
        fd = body_after(core, f"auto vfs_f{operation}_file_as(")
        xfs_case = body_after(fd, "case FSType::XFS:")
        assert xfs_case.index("MutexGuard publication_guard(") < xfs_case.index(f"xfs_f{operation}(f, CREDENTIALS,")
        assert xfs_case.index("if (result == 0)") < xfs_case.index("cache_notify_file_metadata_changed_impl(")


def test_stat_authority_tuples_are_copied_under_writer_locks():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    for name in ["file_stat_snapshot_current", "file_stat_snapshot_promote_prefilled_path",
                 "file_stat_snapshot_promote_created_open_prefill", "file_stat_snapshot_lookup",
                 "metadata_cache_store_opened_file_stat", "vfs_prefill_file_stat_snapshot"]:
        body = body_after(core, f"{name}(")
        assert "MutexGuard cache_guard(file->stat_cache_mutex)" in body, name
        assert body.index("MutexGuard cache_guard(") < body.index("file->stat_cache_" if name != "vfs_prefill_file_stat_snapshot" else "file->stat_cache ="), name
    for name in ["file_stat_snapshot_invalidate", "file_stat_snapshot_store"]:
        body = body_after(core, f"void {name}(")
        assert body.index("MutexGuard cache_guard(") < body.index(f"{name}_locked("), name
    fast = body_after(core, "auto file_stat_snapshot_fast_current(")
    assert fast.index("stat_cache_mutex.try_lock()") < fast.index("auto copy_current") < fast.index("file->stat_cache_valid")
    assert "stat_cache_mutex.lock()" not in fast
    assert fast.index("copy_current();") < fast.index("stat_cache_mutex.unlock()")
    assert "tmpfs_stat_node(" in body_after(core, "auto vfs_fstat_file(")
    assert "tmpfs_stat_node(" in body_after(core, "auto vfs_stream_cache_get_file_stat(")
    legacy_stat = body_after(core, "auto vfs_stat_impl(")
    assert "result == -ENOENT && fill_synthetic_mount_dir_stat(current_path, statbuf) == 0" in legacy_stat
    assert "fill_tmpfs_node_stat_locked(" not in core, "metadata must come from a locked backend copy"
    tmpfs = (ROOT / "modules/kern/src/vfs/fs/tmpfs.cpp").read_text()
    node = body_after(tmpfs, "auto tmpfs_stat_node(")
    assert node.index("MutexGuard guard(tmpfs_lock)") < node.index("tmpfs_fill_stat_locked(")
    inode = (ROOT / "modules/kern/src/vfs/fs/xfs/xfs_inode.cpp").read_text()
    cached = body_after(inode, "auto xfs_inode_stat_cached(")
    assert cached.index("unlock_irqrestore(") < cached.index("MutexGuard metadata_guard(") < cached.index("xfs_inode_fill_stat(")


def test_devfs_metadata_checks_retained_identity_before_publication():
    source = (ROOT / "modules/kern/src/vfs/fs/devfs.cpp").read_text()
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    change = body_after(source, "auto change_node_identity(")
    assert change.index("MetadataGuard const GUARD") < change.index("prepare_file_chmod(")
    assert change.index("prepare_file_chown(") < change.index("if (RET != 0)") < change.index("node->uid = after.uid")
    assert change.index("node->ctime = current_timespec()") < change.index("fill_node_stat_locked(")
    lookup = body_after(source, "auto with_lookup_node(")
    assert lookup.index("OptionalPtsLock const PTS_GUARD") < lookup.index("walk_path(path, false)") < lookup.index("return consume(")
    assert lookup.index("OptionalWkiLock const WKI_GUARD") < lookup.index("walk_path(path, false)")
    assert lookup.index("OptionalNodesLock const NODES_GUARD") < lookup.index("walk_path(path, false)")
    assert "walk_path(path, false) != retained.get()" in lookup and "return -EAGAIN" in lookup
    assert lookup.index("OptionalLocalLock const LOCAL_GUARD") < lookup.index("walk_path(path, false)")
    for signature in ("auto devfs_acquire_path(", "auto devfs_create_directory(",
                      "auto devfs_create_symlink(", "auto devfs_remove_node(",
                      "void devfs_nodes_init(", "void devfs_nodes_add_peer(", "void devfs_nodes_remove_peer("):
        body = body_after(source, signature)
        assert body.index("OptionalNodesLock const NODES_GUARD") < body.index("walk_path("), signature
    writer = body_after(source, "auto devfs_add_device_node(const std::array")
    assert writer.index("OptionalNodesLock const NODES_GUARD") < writer.index("add_device_node_locked(")
    peer = body_after(source, "void devfs_nodes_add_peer(")
    assert peer.count("add_device_node_locked(") == 3 and "devfs_add_device_node(" not in peer
    reader = body_after(source, "auto devfs_readdir(")
    assert reader.index("OptionalNodesLock const NODES_GUARD") < reader.index("dir_node->children_count")
    opened = body_after(source, "auto devfs_open_path(")
    assert opened.index("OptionalNodesLock const NODES_GUARD") < opened.index("node = walk_path(")
    assert opened.index("authorize_file_open(") < opened.index("selected_type = node->type")
    assert opened.index("devfs_file->device = node->device") < opened.index("OPS->open_with_credentials(")
    assert "node->device" not in opened.split("devfs_file->node = node;", 1)[1]

    for name in ("auto devfs_stat_node(", "auto update_node_times(", "void touch_node_modified("):
        assert "MetadataGuard const GUARD" in body_after(source, name), name
    acquire = body_after(core, "auto vfs_acquire_lookup(")
    assert "intent == LookupIntent::METADATA" in acquire and "release_devfs_lookup_cookie" in acquire
    for op in ("chmod", "chown"):
        path = body_after(core, "auto vfs_f" + op + "at(")
        if op == "chown": path += body_after(core, "auto consume_chown_lookup(")
        assert "devfs_" + op + "_lookup(" in path and "handle.authorization().credentials" in path
        fd = body_after(core, "auto vfs_f" + op + "_file_as(")
        block = fd.split("case FSType::DEVFS:", 1)[1].split("case FSType::", 1)[0]
        assert block.index("MutexGuard publication_guard") < block.index("devfs_" + op + "_node(")
        assert block.index("if (result == 0)") < block.index("cache_notify_file_metadata_changed_impl(")
        assert "CREDENTIALS" in block


def test_fat_metadata_authorizes_fixed_projection():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    fat = (ROOT / "modules/kern/src/vfs/fs/fat32.cpp").read_text()
    for op in ("chmod", "chown"):
        resolved = body_after(core, "auto vfs_" + op + "_resolved_path(")
        block = resolved.split("case FSType::FAT32:", 1)[1].split("case FSType::", 1)[0]
        assert "fat32_" + op + "_path(fs_path, CREDENTIALS," in block
        fd = body_after(core, "auto vfs_f" + op + "_file_as(")
        block = fd.split("case FSType::FAT32:", 1)[1].split("case FSType::", 1)[0]
        assert "fat32_f" + op + "(f, CREDENTIALS," in block and "vfs_put_file(" not in block
        backend = body_after(fat, "auto fat32_" + op + "_path(")
        assert backend.index("fat32_stat(") < backend.index("authorize_fixed_file_" + op + "(")
        assert "authorize_fixed_file_" + op + "(" in body_after(fat, "auto fat32_f" + op + "(")
    assert "statbuf->st_mode = fat32_posix_mode(found_attr)" in body_after(fat, "auto fat32_stat(")
    assert "node->mode = fat32_posix_mode(final_entry.attributes) & 07777" in body_after(fat, "auto fat32_open_path_locked(")


def test_timestamp_policy_precedes_locked_mutation_and_transaction():
    tmp = (ROOT / "modules/kern/src/vfs/fs/tmpfs.cpp").read_text()
    dev = (ROOT / "modules/kern/src/vfs/fs/devfs.cpp").read_text()
    xfs = (ROOT / "modules/kern/src/vfs/fs/xfs/xfs_vfs.cpp").read_text()
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    for source, name, write in [(tmp, "auto utimens_locked(", "canonical.atime ="),
                                (dev, "auto update_node_times(", "node->atime ="),
                                (xfs, "auto xfs_set_inode_times(", "ip->atime =")]:
        body = body_after(source, name)
        assert body.index("authorize_file_timestamps(") < body.index(write)
        assert "times.intent()" in body and "get_current_task" not in body
    for op in ("lookup", "node"):
        body = body_after(tmp, "auto tmpfs_utimens_" + op + "(")
        assert body.index("MutexGuard guard(tmpfs_lock)") < body.index("utimens_locked(")
    commit = body_after(xfs, "auto xfs_set_inode_times(")
    assert commit.index("authorize_file_timestamps(") < commit.index("xfs_trans_alloc(")
    assert commit.index("xfs_trans_capture_inode(") < commit.index("ip->atime =") < commit.index("xfs_trans_commit(")
    fd = body_after(xfs, "auto xfs_set_times_file(")
    assert fd.index("io_guard(") < fd.index("XfsMetadataGuard metadata_guard(") < fd.index("xfs_set_inode_times(")
    fd = body_after(core, "auto vfs_futimens_file_as(")
    assert "credentials()" not in fd and "vfs_put_file(" not in fd
    assert fd.index("resolve_utimens_times(") < fd.index("validate_retained_metadata_file(f, resolved_times.changed())") < fd.index("MutexGuard publication_guard(")
    assert fd.index("if (ret == 0 && changed)") < fd.index("cache_notify_file_metadata_changed_impl(")
    for entry, operation in (("vfs_futimens", "utimens"), ("vfs_fchmod_for_task", "chmod"), ("vfs_fchown_for_task", "chown")):
        wrapper = body_after(core, "auto " + entry + "(")
        assert wrapper.count("task->credentials()") == 1
        assert wrapper.index("task->credentials()") < wrapper.index("vfs_get_file_retain(") < wrapper.index("vfs_f" + operation + "_file_as(") < wrapper.index("vfs_put_file(f)")
        assert "publication_guard" not in wrapper
        adapter = body_after(core, "auto vfs_f" + operation + "_file_as(")
        assert "get_current_task" not in adapter and "credentials()" not in adapter and "vfs_put_file(" not in adapter
        assert "vfs_path" not in adapter and "validate_retained_metadata_file(" in adapter
    guard = body_after(core, "auto validate_retained_metadata_file(")
    assert "get_tmpfs_fops()" in guard and "file_mount_read_only(*file)" in guard
    assert "mutation && MUTABLE_NATIVE" in guard
    path = body_after(core, "auto vfs_utimensat(")
    assert "handle.authorization().credentials" in path and "&args.resolved" in path


def test_remote_directory_references_map_admission_and_keep_cleanup_ownership():
    source = (ROOT / "modules/kern/src/net/wki/remote_vfs.cpp").read_text()
    handler = body_after(source, "void handle_vfs_op(").split("case OP_VFS_DIRECTORY_REFERENCE:", 1)[1].split("case OP_VFS_EXEC_VIEW:", 1)[0]
    assert handler.index("credential_record::decode") < handler.index("map_vfs_read_identity") < handler.index("directory_reference_owner(*task, mapped")
    owner = body_after(source, "auto directory_reference_owner(")
    assert "vfs_pin_directory_view_as(task, REQUEST_CREDENTIALS" in owner
    assert "vfs_directory_path_beneath" in owner and "task.credentials()" not in owner
    assert "return -EALREADY" in owner and "row->directory_pin_token == request.token" in owner
    assert owner.index("RemoteFdReservation reservation") < owner.index("vfs_pin_directory_view_as") < owner.index("reservation.publish")
    assert "VISIBLE_NAMESPACE ? export_path : nullptr, VISIBLE_NAMESPACE ? export_name : nullptr" in " ".join(owner.split())
    pin = body_after(source, "auto wki_remote_vfs_pin_directory(")
    assert pin.index("WKI_VFS_FEATURE_DIRECTORY_REFERENCE") < pin.index("allocate_identity") < pin.index("reserve_exec_cancellation") < pin.index("OP_VFS_DIRECTORY_REFERENCE")
    assert "reserve_exec_cancellation(state, TOKEN, true, &IDENTITY)" in pin
    assert "preparation.cancel()" in pin and "remote_vfs_close_remote_fd_best_effort" not in pin
    cancel = body_after(source, "auto cancel_remote_exec_view(")
    assert "record.directory ? OP_VFS_DIRECTORY_REFERENCE : OP_VFS_EXEC_VIEW" in cancel and "&record.identity" in cancel
    retire = body_after(source, "auto cancel_directory_pin(")
    assert retire.index("s_vfs_lock.unlock()") < retire.index("release_server_file")
    release = body_after(source, "auto release_server_file(")
    assert "return ker::vfs::vfs_put_file_result(file)" in release
    assert "FILE_ACCESS_SEARCH_ONLY" not in release and "vfs_close_file" not in release
    cancel_view = body_after(source, "auto cancel_exec_view(")
    assert cancel_view.index("s_vfs_lock.unlock()") < cancel_view.index("vfs_put_file_result(retired)")
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    put = body_after(core, "auto vfs_put_file_result(")
    assert "fetch_sub(1, std::memory_order_acq_rel) == 1" in put
    assert put.index("fetch_sub(") < put.index("return vfs_destroy_file(f)")
    assert "vfs_put_file_result(f)" in body_after(core, "void vfs_put_file(")
    for entry in ("void wki_remote_vfs_gc_stale_fds(", "void wki_remote_vfs_close_marked_server_fds("):
        if entry in source:
            assert "release_server_file" in body_after(source, entry)
    for operation in ("remote_vfs_read", "remote_vfs_write", "remote_vfs_write_impl", "remote_vfs_readdir", "remote_vfs_truncate", "remote_vfs_truncate_impl"):
        assert "FILE_ACCESS_SEARCH_ONLY) return -EBADF" in " ".join(body_after(source, "auto " + operation + "(").split())


def test_owner_directory_pins_retain_complete_views_and_deny_without_refresh():
    source = (ROOT / "modules/kern/src/net/wki/remote_vfs.cpp").read_text()
    owner = body_after(source, "auto directory_reference_owner(")
    assert 'export_path, ".", &file, source.get())' in owner
    assert 'relative_path = "."' in owner
    assert "view = source->directory_view" in owner
    assert "vfs_directory_path_beneath_directory(source.get(), view.root()" in owner
    assert "vfs_directory_path_beneath_directory(file, file->directory_view.root()" in owner
    assert owner.index("reservation.publish") < owner.index("touch_remote_fd") < owner.index("response = result")
    assert "row->file == source.get()" in owner
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    pin = body_after(core, "auto vfs_pin_directory_view_as(")
    assert "auto const REQUEST_CREDENTIALS = credentials" in pin
    assert pin.index("std::unique_ptr<File, LocalWalkFileRelease> pin") < pin.index("MutexGuard publication_guard")
    assert pin.index("vfs_rebase_directory_view") < pin.index("consume_lookup_locked") < pin.index("pin->directory_view = std::move(prepared)")
    assert "pin->mount_policy = handle.mount_policy()" in pin
    assert "*output = pin.release()" in pin
    fixture = body_after(source, "auto wki_remote_vfs_selftest_directory_reference_lifetime(")
    for evidence in ("vfs_rename(EXPORT, RENAMED)", "vfs_chmod(EXPORT, 0000)", "last_activity_us == 1", "vfs_narrow_directory_view", "BUSY - 3", "-EROFS"):
        assert evidence in fixture


def test_retained_directory_name_recovery_preserves_confinement():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    body = body_after(core, "auto vfs_directory_path_beneath(")
    assert "FSType::REMOTE) return -EXDEV" in body
    assert "tmpfs::tmpfs_directory_path" in body and "xfs::xfs_directory_path" in body
    assert "directory->vfs_path" not in body and "copy_directory_lookup_path" not in body
    assert "credentials()" not in body and "get_current_task" not in body
    assert body.index("publication_guard") < body.index("copy_mount_path_exact") < body.index("tmpfs::tmpfs_directory_path")
    assert body.index("path_prefix_matches") < body.index("std::memcpy(buffer") < body.index("*length = LENGTH")
    assert body.index("LENGTH + 1 > size") < body.index("std::memcpy(buffer")
    assert "if (MOUNTS != mount_table_generation_snapshot()) return -EAGAIN" in body


def test_directory_pins_use_search_authority_without_data_grants():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    owner = body_after(core, "auto vfs_pin_directory_resolved_beneath_as(")
    assert "REQUEST_CREDENTIALS = credentials" in owner
    assert "task->credentials()" not in owner and "path_context.credentials()" not in owner
    assert "acquire_confined_owner_lookup(path_context, REQUEST_CREDENTIALS" in owner
    assert "consume_lookup_locked(lookup, false, consume, &context, &path_context, &REQUEST_CREDENTIALS)" in owner
    assert owner.index("lookup_is_beneath_export") < owner.index("consume_lookup_locked")
    assert "vfs_file_clear_path(file)" in owner and "vfs_cache_notify_register_open_file" not in owner
    assert owner.index("result.file->reset(file)") < owner.index("reservation.attach(file)") < owner.index("*output = pin.release()")
    assert "!require_target_search && (directory != nullptr || std::strcmp(path, confinement_root) != 0)" in owner
    assert owner.index("std::unique_ptr<File, LocalWalkFileRelease> pin") < owner.index("MutexGuard publication_guard")
    for name, path in (("tmpfs", "modules/kern/src/vfs/fs/tmpfs.cpp"), ("xfs", "modules/kern/src/vfs/fs/xfs/xfs_vfs.cpp")):
        source = (ROOT / path).read_text()
        pin = body_after(source, "auto " + name + "_pin_directory_lookup(")
        assert "authorize_file_access(credentials" in pin and "FILE_ACCESS_SEARCH_ONLY" in pin
        assert "authorize_file_open" not in pin
        assert "O_CREAT" not in pin and "O_TRUNC" not in pin
        for operation in (("tmpfs_read", "tmpfs_write", "tmpfs_fops_readdir", "tmpfs_fops_truncate") if name == "tmpfs" else
                          ("xfs_vfs_read", "xfs_vfs_write", "xfs_vfs_readdir", "xfs_vfs_truncate")):
            body = body_after(source, "auto " + operation + "(")
            assert "FILE_ACCESS_SEARCH_ONLY) return -EBADF" in " ".join(body.split())
    directory_read = body_after(core, "auto vfs_read_dir_entries(")
    assert directory_read.index("FILE_ACCESS_SEARCH_ONLY") < directory_read.index("HAS_FS_READDIR")


def test_xattr_access_and_visibility_use_explicit_snapshot():
    xfs = (ROOT / "modules/kern/src/vfs/fs/xfs/xfs_vfs.cpp").read_text()
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    for name in ("check_public_xattr_permission", "set_public_xattr_locked", "get_public_xattr_locked",
                 "list_public_xattrs_locked", "remove_public_xattr_locked"):
        body = body_after(xfs, "auto " + name + "(")
        assert "credentials" in body and "get_current_task" not in body and "vfs_check_permission(" not in body
    listing = body_after(xfs, "auto list_public_xattrs_locked(")
    assert listing.index("authorize_file_access(credentials,") < listing.index(".privileged = credentials.allows(") < listing.index("xfs_attr_list(")
    for op in ("set", "remove"):
        mutation = body_after(xfs, "auto " + ("set_public_xattr_locked" if op == "set" else "remove_public_xattr_locked") + "(")
        assert mutation.index("check_public_xattr_permission(credentials,") < mutation.index("xfs_trans_alloc(")
        fd = body_after(xfs, "auto xfs_f" + op + "xattr(")
        assert fd.index("io_guard(") < fd.index("XfsMetadataGuard metadata_guard(")
        retained = body_after(xfs, "auto xfs_" + op + "xattr_lookup(")
        assert retained.index("io_guard.emplace(") < retained.index("XfsMetadataGuard metadata_guard(") < retained.index("xfs_lookup_target_locked(")
    for op in ("set", "get", "list", "remove"):
        fd = body_after(core, "auto vfs_f" + op + "xattr(")
        assert fd.count("task->credentials()") == 1
        assert fd.index("task->credentials()") < fd.index("vfs_get_file_retain(") < fd.index("dispatch_xattr_file(credentials,") < fd.index("vfs_put_file(file)")
    path = body_after(core, "auto dispatch_xattr_stable(")
    assert path.count("task->credentials()") == 1
    assert path.index("task->credentials()") < path.index("for (unsigned attempt")
    assert "&lookup, 0, false, &REQUEST_CREDENTIALS" in path
    assert "consume_lookup_locked(lookup, false, consume_xattr_lookup, &context, task, &REQUEST_CREDENTIALS)" in path
    consume = body_after(core, "auto consume_xattr_lookup(")
    assert "handle.authorization().credentials" in consume and "dispatch_xattr_path(credentials," in consume
    assert "else if (!args.confined)" in consume
    assert consume.index("handle.mount_policy().read_only") < consume.index("xfs_setxattr_lookup(")
    assert consume.index("xfs_stat_lookup(backend, true, &metadata)") < consume.index("xfs_setxattr_lookup(")
    owner = body_after(core, "auto vfs_xattr_resolved_beneath_as(")
    assert "current_metadata_credentials" not in owner and "task->credentials" not in owner
    assert owner.index("REQUEST_CREDENTIALS = credentials") < owner.index("for (unsigned attempt")
    assert owner.index("acquire_confined_owner_lookup(") < owner.index("publication_guard(") < owner.index("lookup_is_beneath_export(") < owner.index("consume_lookup_locked(")
    assert "&path_context, &REQUEST_CREDENTIALS" in owner and ".confined = true" in owner
    retained_owner = body_after(core, "auto vfs_xattr_file_as(")
    assert retained_owner.index("FSType::REMOTE") < retained_owner.index("dispatch_xattr_file(REQUEST_CREDENTIALS,")
    assert "current_metadata_credentials" not in retained_owner and "get_current_task" not in retained_owner
    fd = body_after(core, "auto dispatch_xattr_file(")
    assert "MUTATION && file->fs_type == FSType::XFS" in fd
    assert fd.index("publication_guard.emplace(") < fd.index("xfs_fsetxattr(credentials,") < fd.index("cache_notify_file_metadata_changed_impl(")


def test_tmpfs_open_authorization_precedes_publication_and_truncation():
    tmpfs = (ROOT / "modules/kern/src/vfs/fs/tmpfs.cpp").read_text()
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    opened = body_after(tmpfs, "auto tmpfs_open_lookup(")
    assert opened.index("MutexGuard guard(tmpfs_lock)") < opened.index("tmpfs_validate_lookup_locked(handle)")
    assert opened.index("prepare_file_create(credentials,") < opened.index("auto* file = new File;")
    assert opened.index("authorize_file_open(credentials,") < opened.index("auto* file = new File;")
    assert opened.index("auto* file = new File;") < opened.index("tmpfs_create_file_with_identity(") < opened.index("tmpfs_resize_locked(")
    assert opened.index("tmpfs_resize_locked(") < opened.index("open_count.fetch_add(")
    creator = body_after(tmpfs, "auto tmpfs_create_file_with_identity(")
    assert creator.index("node->uid = identity.uid") < creator.index("add_child(parent, node)")
    assert creator.index("node->gid = identity.gid") < creator.index("add_child(parent, node)")
    trunc = body_after(core, "auto apply_open_truncation(")
    assert "f->fs_type == FSType::TMPFS && f->fops == ker::vfs::tmpfs::get_tmpfs_fops()" in trunc
    for name in ("auto vfs_open_resolved_for_task(", "static auto vfs_open_file_impl("):
        opened = body_after(core, name)
        assert "stable_lookup->authorization().credentials" in opened
        assert "tmpfs_open_lookup(CREDENTIALS," in opened and "tmpfs_open_path(CREDENTIALS," in opened
        assert "backend_open_result == -ENOENT || backend_open_result == -ENOSYS" in opened
        assert "vfs_check_permission_for_task(" not in opened


def test_xfs_open_authorization_precedes_creation_and_file_publication():
    xfs = (ROOT / "modules/kern/src/vfs/fs/xfs/xfs_vfs.cpp").read_text()
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    for name in ("auto xfs_open_path(", "auto xfs_open_lookup("):
        body = body_after(xfs, name)
        assert "get_current_task" not in body
        assert body.index("XfsMetadataGuard metadata_guard(") < body.index("authorize_file_access(")
        assert body.index("prepare_file_create(") < body.index("!storage.prepare()") < body.index("xfs_trans_alloc(")
        assert body.index("new_inode->uid = identity.uid") < body.index("xfs_dir_addname(")
        assert body.index("new_inode->gid = identity.gid") < body.index("xfs_dir_addname(")
        assert "PERMS == 0" not in body
        assert body.index("authorize_file_open(") < body.index("xfs_file_from_inode(storage,")
    retained = body_after(xfs, "auto xfs_open_lookup(")
    assert retained.index("xfs_validate_lookup_locked(handle)") < retained.index("authorize_file_access(")
    for name in ("auto vfs_open_resolved_for_task(", "static auto vfs_open_file_impl("):
        body = body_after(core, name)
        assert "stable_lookup->authorization().credentials" in body
        assert "xfs_open_lookup(CREDENTIALS," in body and "xfs_open_path(CREDENTIALS," in body


def test_fat_open_denials_precede_creation_and_propagate_errno():
    fat = (ROOT / "modules/kern/src/vfs/fs/fat32.cpp").read_text()
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    body = body_after(fat, "auto fat32_open_path_locked(")
    assert "get_current_task" not in body
    assert body.index("authorize_file_access(credentials, {0, 0, 0040755}, 3)") < body.index("create_file_in_directory(")
    assert body.index("authorize_file_open(credentials,") < body.index("auto* node = new FAT32Node;")
    assert "return fail(-EACCES)" not in body  # All access decisions use the central policy result.
    assert "return fail(-EEXIST)" in body and "return fail(-ENOTDIR)" in body
    creator = body_after(fat, "static auto create_file_in_directory(")
    assert creator.index("auto* node = new FAT32Node;") < creator.index("ker::dev::block_write(")
    assert creator.index("auto* f = new File;") < creator.index("ker::dev::block_write(")
    for name in ("auto vfs_open_resolved_for_task(", "static auto vfs_open_file_impl("):
        opened = body_after(core, name)
        assert "fat32_open_path(CREDENTIALS," in opened
        start = opened.index("fat32_open_path(CREDENTIALS,")
        assert "&backend_open_result" in opened[start:start + 500]


def test_devfs_open_checks_before_callbacks_and_readlink_has_no_open():
    dev = (ROOT / "modules/kern/src/vfs/fs/devfs.cpp").read_text()
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    body = body_after(dev, "auto devfs_open_path(")
    assert "get_current_task" not in body
    assert body.index("MetadataGuard const GUARD(*node)") < body.index("authorize_file_open(credentials,")
    callback = "OPS->open_with_credentials(file, credentials, opener)"
    assert body.index("authorize_file_open(credentials,") < body.index("node->wki_open_refs++") < body.index(callback)
    assert body.index("if (policy != 0)") < body.index("node->pts_refs.fetch_add(") < body.index(callback)
    assert "return fail(OPEN_RESULT < 0 ? OPEN_RESULT : -EIO)" in body
    link = body_after(dev, "auto devfs_readlink_path(")
    assert "devfs_acquire_path(path)" in link and "devfs_open_path" not in link
    assert "return retained.readlink(buf, bufsize)" in link
    pinned = body_after(dev, "auto DevFSNodeRef::readlink(")
    assert pinned.index("OptionalWkiLock const WKI_GUARD(owns_wki_node_ref_)") < pinned.index("MetadataGuard const GUARD(*node_)") < pinned.index("std::memcpy")
    assert "devfs_open" not in pinned and "devfs_acquire_path" not in pinned
    for name in ("auto vfs_open_resolved_for_task(", "static auto vfs_open_file_impl("):
        opened = body_after(core, name)
        assert "devfs_open_path(CREDENTIALS," in opened
    assert "devfs_readlink_path(strip_mount_prefix(mount, abs_path), buf, bufsize)" in core


def test_procfs_global_policy_covers_types_and_precedes_content_or_controls():
    proc = (ROOT / "modules/kern/src/vfs/fs/procfs.cpp").read_text()
    header = (ROOT / "modules/kern/src/vfs/fs/procfs.hpp").read_text()
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    enum = body_after(header, "enum class ProcNodeType")
    types = set(re.findall(r"^\s*([A-Z][A-Z_]+)\s*[,=]", enum, re.M))
    policy = body_after(proc, "constexpr auto diagnostic_access(")
    assert set(re.findall(r"case ProcNodeType::([A-Z_]+):", policy)) == types
    assert "return DiagnosticAccess::DENY" in policy
    read = body_after(proc, "auto procfs_read_with_credentials(")
    assert read.index("authorize_diagnostic(credentials,") < read.index("MutexGuard content_guard(") < read.index("if (pfd->content == nullptr)")
    write = body_after(proc, "auto procfs_write_with_credentials(")
    assert write.index("authorize_diagnostic(credentials,") < write.index("MutexGuard content_guard(") < write.index("if (count == 0)")
    for token in ("wki_chaos_workload_configure(", "wki_chaos_configure(", "procfs_write_memacc_track(", "start_recording(mask)", "ker::mod::perf::disable()", "set_event_mask(MASK)", "delete[] pfd->content"):
        assert write.index("if (POLICY != 0)") < write.index(token)
    for name in ("auto procfs_read(", "auto procfs_write("):
        assert body_after(proc, name).count("current_procfs_credentials()") == 1
    opened = body_after(proc, "auto authorize_procfs_open_as(")
    assert "authorize_diagnostic(credentials," in opened and "get_current_task" not in opened
    for name in ("auto vfs_open_resolved_for_task(", "static auto vfs_open_file_impl("):
        assert "procfs_open_path_as(CREDENTIALS," in body_after(core, name)
    assert "procfs_open_path(fs_path" not in core


def test_procfs_metadata_retains_owner_and_stages_before_publication():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    proc = (ROOT / "modules/kern/src/vfs/fs/procfs.cpp").read_text()
    parser = body_after(proc, "auto procfs_lookup_path_as(")
    assert "pid != 0 || private_procfs_node(type)" in parser
    assert "pfd->target == nullptr && (PRIVATE_VIEW || type != ProcNodeType::SELF_LINK)" in parser
    stat = body_after(proc, "auto procfs_fill_stat(")
    assert stat.count("credential_observation()") == 1
    assert "owner_task = pfd->target" in stat and "*target_generation = owner.generation" in stat
    for token in ("find_task_by_pid", "authorize_private_procfs", "current_procfs", "release()"):
        assert token not in stat
    consumed = body_after(core, "auto metadata_procfs_lookup_as(")
    assert consumed.index("procfs::procfs_fill_stat(") < consumed.index("MutexGuard publication_guard")
    assert consumed.index("procfs_revalidate_metadata_result(") < consumed.index("*args.output = args.candidate")
    assert "lookup.authorization().credentials" in consumed and "authorize_file_access(" in consumed
    assert "consume_lookup_locked(handle, false, publish, &context, &task, &credentials)" in consumed
    for token in ("procfs_lookup_path", "find_task_by_pid", "vfs_stat_resolved", "get_current_task", "task.credentials()"):
        assert token not in consumed
    for name, args in (("vfs_statat", "statbuf, 0"), ("vfs_faccessat", "nullptr, mode")):
        entry = body_after(core, f"auto {name}(")
        assert f"metadata_procfs_lookup_as(lookup, *task, REQUEST_CREDENTIALS, {args})" in entry
    for name in ("acquire_local_component_lookup", "vfs_acquire_lookup"):
        entry = body_after(core, f"auto {name}(")
        binding = entry[entry.index("intent == LookupIntent::READLINK || intent == LookupIntent::OPEN"):]
        assert "intent == LookupIntent::STAT" in binding and "intent == LookupIntent::ACCESS" in binding


def test_procfs_open_retains_identity_and_publishes_only_after_validation():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    proc = (ROOT / "modules/kern/src/vfs/fs/procfs.cpp").read_text()
    policy = body_after(proc, "auto authorize_procfs_open_as(")
    assert "authorize_private_procfs(credentials, caller_pid, *data, observation)" in policy
    assert policy.index("procfs_lookup_is_symlink(file)") < policy.index("authorize_private_procfs(")
    cloned = body_after(proc, "auto procfs_open_lookup_as(")
    assert cloned.index("authorize_procfs_open_as(") < cloned.index("new ProcFileData")
    for token in ("data->node = source.node", "source.target->try_acquire()", "data->target = source.target", "new File{}"):
        assert token in cloned
    for token in ("procfs_lookup_path", "current_procfs_pid", "get_current_task", "memcpy"):
        assert token not in cloned
    path = body_after(proc, "auto procfs_open_path_as(")
    assert "procfs_lookup_path_as(path, caller_pid, flags, caller_task, &lookup_result)" in path
    assert "authorize_procfs_open_as(credentials, caller_pid," in path
    assert "current_procfs_pid" not in path
    consumed = body_after(core, "auto open_procfs_lookup_as(")
    assert consumed.index("procfs_lookup_is_symlink(retained)") < consumed.index("descriptor.emplace(&task)")
    assert consumed.index("procfs::procfs_open_lookup_as(") < consumed.index("MutexGuard publication_guard")
    assert consumed.index("procfs_revalidate_open_result(") < consumed.index("args.descriptor->install(")
    assert "procfs::procfs_revalidate_open_result(args.identity, args.generation)" in consumed
    assert "procfs::procfs_open_fd_lookup_as(credentials, caller_pid, retained," in consumed
    duplicated = body_after(proc, "auto procfs_open_fd_lookup_as(")
    assert duplicated.index("authorize_private_procfs(") < duplicated.index("authorized_task_fd_retain(")
    assert "*data.target, data.node.fd" in duplicated
    assert "revalidate_private_procfs(data, before)" in duplicated
    for token in ("find_task_by_pid", "procfs_lookup_path", "current_procfs", "open_flags =", "vfs_path ="):
        assert token not in duplicated
    assert "consume_lookup_locked(handle, false, publish, &context, &task, &credentials)" in consumed
    assert consumed.index("file.release()") < consumed.index("vfs_cache_notify_path_changed(handle.path()")
    for token in ("readlink_resolved", "procfs_open_path", "get_current_task", "task.credentials()", "apply_open_truncation"):
        assert token not in consumed
    opened = body_after(core, "static auto open_at_for_task(")
    assert "open_procfs_lookup_as(lookup, *task, REQUEST_CREDENTIALS, CALLER_PID, flags, context.directory_view, output)" in opened
    assert "ker::mod::sched::task::process_pid(*task)" in opened


def test_procfs_final_readlink_retains_request_identity_and_stages_before_publication():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    proc = (ROOT / "modules/kern/src/vfs/fs/procfs.cpp").read_text()
    parser = body_after(proc, "auto procfs_lookup_path_as(")
    assert "SELF_PID = PRIVATE_VIEW ? ker::mod::sched::task::namespace_process_pid(*caller_task) : caller_pid" in parser
    assert "get_current_task" not in parser and "current_procfs_pid" not in parser
    stat = body_after(core, "auto local_walk_read_stat(")
    assert "procfs_lookup_path_as(fs_path, caller_pid, 0, caller_task, &lookup_result)" in stat
    walker = body_after(core, "auto acquire_local_component_lookup(")
    assert "current.proc && (intent == LookupIntent::READLINK || intent == LookupIntent::OPEN ||" in walker
    assert "current.mount->fs_type == FSType::PROCFS &&" in walker
    assert "auto* backend = current.proc.release()" in walker
    assert "backend, release_procfs_lookup_cookie" in walker
    fallback = body_after(core, "auto vfs_acquire_lookup(")
    assert fallback.index("procfs::procfs_lookup_path_as(") < fallback.index("g_vfs_namespace_publication_mutex.lock()")
    consumed = body_after(core, "auto readlink_procfs_lookup_as(")
    assert "static_cast<File*>(handle.backend_parent())" in consumed
    assert consumed.index("procfs::procfs_readlink_as(") < consumed.index("MutexGuard publication_guard")
    assert "credentials, caller_pid, &context.target_generation" in " ".join(consumed.split())
    assert consumed.index("procfs_revalidate_readlink_result(") < consumed.index("std::memcpy(args.output")
    assert "consume_lookup_locked(handle, false, publish, &context, &task, &credentials)" in consumed
    for forbidden in ("procfs_lookup_path", "readlink_resolved", "get_current_task", "task.credentials()"):
        assert forbidden not in consumed
    public = body_after(core, "auto vfs_readlinkat(")
    assert "CALLER_PID = ker::mod::sched::task::process_pid(*task)" in public
    assert "readlink_procfs_lookup_as(lookup, *task, REQUEST_CREDENTIALS, CALLER_PID, buf, bufsize)" in public
    validation = body_after(proc, "auto procfs_revalidate_readlink_result(")
    assert "revalidate_private_procfs(data, before)" in validation
    assert "target_generation == 0" in validation


def test_procfs_root_consumers_use_canonical_value_snapshot():
    proc = (ROOT / "modules/kern/src/vfs/fs/procfs.cpp").read_text()
    snapshot = body_after(proc, "auto copy_proc_filesystem_root(")
    assert snapshot.index("task.filesystem_owner()") < snapshot.index("if (owner == nullptr) return -ESRCH")
    assert snapshot.index("owner->path_metadata_lock.lock_irqsave()") < snapshot.index("output = owner->root")
    assert snapshot.index("output = owner->root") < snapshot.index("owner->path_metadata_lock.unlock_irqrestore(")
    for forbidden in ("find_task", "get_current_task", "capture_filesystem_context", "DirectoryAnchor", "vfs_put_file"):
        assert forbidden not in snapshot
    link = body_after(proc, "auto procfs_readlink_as_impl(")
    assert link.count("copy_proc_filesystem_root(*task, root)") == 1
    assert "vfs_getroot_for_task(task, staged.data()" in link
    assert "task->root" not in link
    assert link.index("authorize_private_procfs(") < link.index("copy_proc_filesystem_root(")
    assert link.index("revalidate_private_procfs(") < link.index("std::memcpy(buf, staged.data(), LENGTH)")
    mounts = body_after(proc, "auto generate_mounts_for_task(")
    assert mounts.index("vfs_capture_root_metadata(task, root)") < mounts.index("get_mount_snapshot_at(")
    assert "task->root" not in mounts and "get_current_task" not in mounts
    fixture = body_after(proc, "auto selftest_canonical_filesystem_root(")
    for token in ("/obsolete-thread-root", "bind_thread_owner(thread, process)", "ProcNodeType::ROOT_LINK", "ProcNodeType::FD_LINK",
                  "UINT64_MAX - 1", "output == SENTINEL", "generation == UINT64_MAX", "TaskState::EXITING", "TaskState::DEAD",
                  "description.refcount.load() == 1", "owner_mounts == thread_mounts", "owner_mounts != global_mounts"):
        assert token in fixture
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    owner = body_after(core, "auto fd_table_task_for(")
    assert "if (task->is_thread)" in owner and "task->credential_owner()" in owner
    assert "owner != nullptr && owner->try_acquire()" in owner and "return {owner, true}" in owner
    assert "find_task_by_pid" not in owner and "owner_pid" not in owner
    for token in ("vfs_get_file_retain(&unbound_thread, 13)", "vfs_alloc_fd(&unbound_thread, &description) == -ESRCH",
                  "vfs_release_fd(&unbound_thread, 13) == -ESRCH", "unbound_thread.get_fd_cloexec(13)",
                  "vfs_alloc_fd(&thread, &allocated)", "vfs_release_fd(&thread, ALLOCATED_FD)"):
        assert token in fixture
    ktest = (ROOT / "modules/kern/src/test/credential_ktest.cpp").read_text()
    assert "ProcfsRootAndFdLinksAndMountsUseCanonicalFilesystemOwner" in ktest


def test_procfs_fd_access_authorizes_and_revalidates_before_publication():
    proc = (ROOT / "modules/kern/src/vfs/fs/procfs.cpp").read_text()
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    retained = body_after(proc, "auto authorized_task_fd_retain(")
    assert retained.index("may_access_process_private(") < retained.index("task_fd_file_retain(&target, fd)")
    assert retained.index("task_fd_file_retain(&target, fd)") < retained.index("AFTER.generation != BEFORE.generation") < retained.index("return file;")
    assert "ker::vfs::vfs_put_file(file)" in retained and "result = -EAGAIN" in retained
    assert "SharedVmemPublicationGuard" not in retained
    link = body_after(proc, "auto procfs_readlink_as_impl(")
    assert "current_procfs_credentials()" not in link and "get_current_task" not in link
    explicit = body_after(proc, "auto procfs_readlink_as(")
    assert "procfs_readlink_as_impl(file, buffer, capacity, credentials, caller_pid, target_generation, allow_remote_cwd, nullptr, caller_task)" in " ".join(explicit.split())
    assert link.index("authorized_task_fd_retain(") < link.index("file->path_metadata_lock.lock_irqsave()")
    opened = body_after(proc, "auto procfs_open_fd_link_path(")
    assert "get_current_task" not in opened
    assert "authorized_task_fd_retain(credentials, caller_pid, *task, parsed.fd, *result_out)" in opened
    fallback = body_after(core, "auto vfs_try_open_procfs_fd_link(")
    assert fallback.index("if (file != nullptr || result != 0)") < fallback.index("resolve_symlinks(")
    assert "result = RESOLVE_RET" in fallback
    assert "intent == LookupIntent::OPEN" in body_after(core, "auto vfs_acquire_lookup(")
    confined = body_after(core, "auto lookup_is_beneath_export(")
    assert "lookup.intent() == LookupIntent::OPEN" in confined and "procfs_path_is_fd_link(" in confined
    assert confined.index("procfs_path_is_fd_link(") < confined.index("path_prefix_matches(")
    assert "procfs_path_is_fd_link(strip_mount_prefix(prefix_mount, path))" in body_after(core, "auto resolve_prefix_symlink_once(")
    prefix = body_after(core, "auto resolve_prefix_symlink_once(")
    assert "LINK_LEN < 0 && LINK_LEN != -EINVAL && LINK_LEN != -ENOSYS && LINK_LEN != -ENOENT && LINK_LEN != -ENOTDIR" in prefix
    resolved = body_after(core, "auto resolve_symlinks(")
    start = resolved.index("if (mount->fs_type == FSType::PROCFS)")
    assert "if (LINK_LEN < 0) return static_cast<int>(LINK_LEN)" in resolved[start:]

    for name in ("auto vfs_open_resolved_for_task(", "static auto vfs_open_file_impl("):
        body = body_after(core, name)
        assert body.index("if (fd_link_result != 0)") < body.index("if (fd_link_file != nullptr)")


def test_private_procfs_pins_identity_and_revalidates_cache_links_and_enumeration():
    proc = (ROOT / "modules/kern/src/vfs/fs/procfs.cpp").read_text()
    header = (ROOT / "modules/kern/src/vfs/fs/procfs.hpp").read_text()
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    assert "Task* target{}" in header
    assert "target->release()" in body_after(proc, "ProcFileData::~ProcFileData()")
    assert "pfd->target = retain_target(pid)" in body_after(proc, "auto procfs_lookup_path_as(")
    parser = body_after(proc, "auto procfs_lookup_path_as(")
    assert "pid == caller_pid && pid == owner->pid" in parser
    assert "owner->try_acquire() ? owner : nullptr" in parser
    assert "auto* group_target = retain_target(static_cast<uint64_t>(PID))" in parser
    assert "task_group_pid_for(" not in parser
    assert "return ker::mod::sched::find_task_by_pid_safe(pid)" in parser
    policy = body_after(proc, "auto authorize_private_procfs(")
    assert "credential_observation()" in policy and "may_access_process_private(" in policy
    private = body_after(proc, "auto procfs_read_private(")
    assert private.index("authorize_private_procfs(") < private.index("new (std::nothrow)")
    assert "content_credential_generation != before.generation" in private
    assert private.index("revalidate_private_procfs(") < private.index("delete[] data.content") < private.index("std::memcpy(buf,")
    for generator in ("generate_images", "generate_maps", "generate_cmdline"):
        body = body_after(proc, f"auto {generator}(")
        assert "find_task_by_pid_safe" not in body and "SharedVmemPublicationGuard" in body
        if generator != "generate_cmdline":
            assert body.index("SharedVmemPublicationGuard") < body.index("ProcPagemapPin pin(*task)")
    link = body_after(proc, "auto procfs_readlink_as_impl(")
    assert "find_task_by_pid_safe" not in link and "SharedVmemPublicationGuard" not in link
    assert link.index("authorize_private_procfs(") < link.index("task->path_metadata_lock.lock_irqsave()")
    # Missing canonical ownership releases the retained descriptor before any
    # File path lock; normal cleanup follows that lock's release.
    assert link.count("ker::vfs::vfs_put_file(file)") == 2
    assert link.index("ker::vfs::vfs_put_file(file)") < link.index("file->path_metadata_lock.lock_irqsave()")
    assert link.index("file->path_metadata_lock.unlock_irqrestore(") < link.rindex("ker::vfs::vfs_put_file(file)")
    assert link.index("revalidate_private_procfs(") < link.index("std::memcpy(buf, staged.data()")
    entries = body_after(proc, "auto procfs_readdir_private(")
    assert entries.index("authorize_private_procfs(") < entries.index("if (count < 2)")
    assert entries.index("revalidate_private_procfs(") < entries.index("*buf = staged")
    outer = body_after(proc, "auto procfs_readdir(")
    assert outer.index("procfs_readdir_private(") < outer.index("if (count == 0)")
    listing = body_after(core, "auto vfs_read_dir_entries(")
    assert "f->fs_type == FSType::PROCFS || std::cmp_equal(f->dir_fs_count, -1)" in listing
    assert listing.index("f->fs_type == FSType::PROCFS && RET != -ENOENT") < listing.index("f->dir_fs_count = ACTUAL_INDEX")
    seek = body_after(proc, "auto procfs_lseek_with_credentials(")
    assert seek.index("authorize_private_procfs(") < seek.index("pfd->content_len") < seek.index("f->pos = new_pos")
    for name in ("vfs_file_set_path", "vfs_file_clear_path"):
        path = body_after(core, f"{'void' if name.endswith('clear_path') else 'auto'} {name}(")
        assert path.index("path_metadata_lock.unlock_irqrestore(") < path.index("delete[] retired")
    exec_source = (ROOT / "modules/kern/src/syscalls_impl/process/exec.cpp").read_text()
    commit = exec_source[exec_source.index("if (commit_published) {"):]
    assert commit.index("path_metadata_lock.lock_irqsave()") < commit.index("publish_exec_credentials(") < commit.index("path_metadata_lock.unlock_irqrestore(")

def test_device_open_uses_explicit_task_authority():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    resolved = body_after(core, "auto vfs_open_resolved_for_task(")
    assert "OPEN_REQUIRE_DIRECTORY, task_is_opener ? task : nullptr" in resolved
    ordinary = body_after(core, "static auto vfs_open_file_impl(")
    assert "OPEN_REQUIRE_DIRECTORY, apply_task_policy ? CALLER : nullptr" in ordinary
    explicit = body_after(core, "auto vfs_open_file_as(")
    assert "&handle, &args.file, false, &args.directory_view)" in explicit
    devfs = (ROOT / "modules/kern/src/vfs/fs/devfs.cpp").read_text()
    opened = body_after(devfs, "auto devfs_open_path(")
    assert "OPS->open_with_credentials(file, credentials, opener)" in opened
    assert "get_current_task" not in opened
    console = (ROOT / "modules/kern/src/dev/console.cpp").read_text()
    terminal = body_after(console, "int tty_open(")
    assert "get_current_task" not in terminal
    assert terminal.count("task->job_identity()") == 1
    assert "task->clear_controlling_terminal_if(TERMINAL, JOB.terminal_generation)" in terminal
    assert terminal.index("pty_open_controlling_slave(TERMINAL, JOB.session_id, JOB.terminal_generation)") < terminal.index("dff->device =")
    assert ".open_with_credentials = tty_open" in body_after(console, "CharDeviceOps tty_ops =")


def test_journal_device_open_consumes_devfs_admission_snapshot():
    publication_source = (ROOT / "modules/kern/src/platform/dbg/journal.cpp").read_text()
    note = body_after(publication_source, "void note_module_registration_locked(")
    assert "s_registration_pending.store(true" in note
    assert "dev_register(" not in note and "devfs_" not in note.replace("s_devfs_ready", "").replace("devfs_registered", "")
    for name in ("void emit(", "void set_module_output_devices("):
        emission = body_after(publication_source, name)
        assert "note_module_registration_locked(" in emission
        assert "dev_register(" not in emission and "devfs_add_device_node(" not in emission
    publisher = body_after(publication_source, "void publish_pending_modules(")
    assert publisher.index("unlock_irqrestore(flags)") < publisher.index("dev_register(") < publisher.index("devfs_add_device_node(")
    assert "s_registration_publishing.test_and_set" in publisher and "entry.devfs_registered = true" in publisher
    for name in ("void register_devices(", "void mark_devfs_ready("):
        bootstrap = body_after(publication_source, name)
        assert bootstrap.index("unlock_irqrestore(FLAGS)") < bootstrap.index("publish_pending_modules()")
    journal = (ROOT / "modules/kern/src/platform/dbg/journal.cpp").read_text()
    operations = body_after(journal, "ker::dev::CharDeviceOps s_journal_ops =")
    assert ".open_with_credentials = journal_open_with_credentials" in operations
    assert ".open =" not in operations
    opened = body_after(journal, "auto journal_open_with_credentials(")
    assert "caller_credentials()" not in opened and "get_current_task" not in opened
    assert opened.index("authorize_read(caller)") < opened.index("file->pos = NO_SEQUENCE")
    assert "entry.device.char_ops = &s_journal_ops" in body_after(journal, "void publish_pending_modules(")
    for name in ("read_records", "poll_check", "poll_register_waiter"):
        assert "caller_credentials()" in body_after(journal, f"auto {name}(")


def test_resource_owner_uses_immutable_credential_subject_before_policy():
    source = (ROOT / "modules/kern/src/platform/sched/resource.cpp").read_text()
    owner = body_after(source, "Owner::Owner(")
    assert "subject->is_thread" in owner
    assert "find_task_by_pid_safe" not in owner and "subject->owner_pid" not in owner
    assert owner.index("subject->credential_owner()") < owner.index("m_task->try_acquire()") < owner.index("if (!m_retained) m_task = nullptr")
    release = body_after(source, "Owner::~Owner(")
    assert release.index("if (m_retained)") < release.index("m_task->release()")
    limits = body_after(source, "auto syscall_limit(")
    assert limits.index("auto const CREDENTIALS = caller->credentials()") < limits.index("Owner owner(caller)") < limits.index("process->resources.set(")


def test_procfs_limits_keep_admitted_caller_and_retained_subject():
    proc = (ROOT / "modules/kern/src/vfs/fs/procfs.cpp").read_text()
    read = body_after(proc, "auto procfs_read_with_credentials(")
    assert "procfs_read_limits(credentials, caller_pid, *pfd, buf, count, offset)" in read
    limits = body_after(proc, "auto procfs_read_limits(")
    for forbidden in ("get_current_task", "->credentials()", "find_task_by_pid_safe", "subject->release()"):
        assert forbidden not in limits
    authority = body_after(proc, "auto authorize_procfs_limits(")
    assert "auto* subject = data.target" in authority and "subject->credential_owner()" in authority
    assert authority.count("TaskState::ACTIVE") == 2
    assert "find_task_by_pid_safe" not in authority and "Owner owner" not in limits
    assert "credentials.allows(ker::mod::security::Authority::RESOURCE_LIMIT)" in authority
    assert "caller_pid == 0 || caller_pid != owner->pid" in authority
    policy = limits.index("authorize_procfs_limits(credentials, caller_pid, data, process)")
    assert policy < limits.index("process->fd_table_lock.lock_irqsave()") < limits.index("fd.current =")
    assert limits.index("if (POLICY != 0)") < limits.index("new (std::nothrow)") < limits.index("std::memcpy(buf,")
    assert limits.index("if (POLICY != 0)") < limits.index("if (offset >= data.content_len)")
    seek = body_after(proc, "auto procfs_lseek_with_credentials(")
    assert seek.index("authorize_procfs_limits(CREDENTIALS, caller_pid, *pfd, process)") < seek.index("pfd->content_len") < seek.index("f->pos = new_pos")
    for forbidden in ("get_current_task", "current_procfs_credentials", "current_procfs_pid", "new ", "procfs_read_limits("):
        assert forbidden not in seek
    entry = body_after(proc, "auto procfs_lseek(")
    assert "procfs_lseek_with_credentials(current_procfs_credentials(), current_procfs_pid(), f, offset, whence, task)" in entry


def test_public_procfs_uses_pinned_memory_and_bounded_names_before_cache_publication():
    proc = (ROOT / "modules/kern/src/vfs/fs/procfs.cpp").read_text()
    memory = body_after(proc, "auto collect_proc_memory(")
    assert memory.index("SharedVmemPublicationGuard") < memory.index("ProcPagemapPin pin(task)") < memory.index("collect_user_memory_stats(pin.pagemap)")
    for name in ("generate_status", "generate_stat", "generate_statm", "generate_meminfo"):
        body = body_after(proc, f"auto {name}(")
        assert "collect_proc_memory(*task)" in body and "task->pagemap" not in body
        if name in ("generate_status", "generate_stat"):
            assert "copy_proc_public_comm(*task)" in body and "task->name" not in body and "task->exe_path" not in body
    for name in ("collect_memacc_process_totals", "generate_memacc_procs"):
        body = body_after(proc, f"auto {name}(")
        assert body.index("SharedVmemPublicationGuard") < body.index("ProcPagemapPin pin(*task)") < body.index("task_memacc_layout_locked(*task)")
        assert "collect_user_memory_breakdown(pin.pagemap, LAYOUT)" in body and "collect_user_memory_stats(pin.pagemap)" in body
    names = body_after(proc, "auto copy_proc_names_locked(")
    assert names.index("path_metadata_lock.lock_irqsave()") < names.index("names.executable = task.exe_path") < names.index("path_metadata_lock.unlock_irqrestore(")
    comm = body_after(proc, "auto copy_proc_public_comm(")
    assert comm.index("SharedVmemPublicationGuard") < comm.index("copy_proc_names_locked(task)")
    assert "if (*p == '/') comm = p + 1" in comm and "BYTE < 0x20 || BYTE == 0x7f" in comm
    read = body_after(proc, "auto procfs_read_with_credentials(")
    assert read.index("procfs_read_private(") < read.index("MutexGuard content_guard(")
    assert read.index("procfs_read_limits(") < read.index("MutexGuard content_guard(") < read.index("new (std::nothrow)")
    for name in ("status", "stat", "statm", "wki_launcher", "wki_runner", "wki_remote_pid"):
        body = body_after(proc, f"auto generate_{name}(")
        assert "find_task_by_pid_safe" not in body and "task->release()" not in body
        assert "task == nullptr" in body and "TaskState::ACTIVE" in body
        assert f"generate_{name}(pfd->target," in read
    seek = body_after(proc, "auto procfs_lseek_with_credentials(")
    assert seek.index("MutexGuard content_guard(") < seek.index("pfd->content_len")



def test_wki_procfs_identity_copies_pin_tasks_and_release_locks_before_formatting():
    proc = (ROOT / "modules/kern/src/vfs/fs/procfs.cpp").read_text()
    assert "find_task_by_pid(" not in proc
    for name in ("generate_wki_launcher", "generate_wki_runner", "generate_wki_remote_pid"):
        body = body_after(proc, f"auto {name}(")
        capture = "wki_proxy_task_remote_info(" if name.endswith("runner") else "wki_execution_identity()"
        assert "find_task_by_pid_safe(" not in body and "task->release()" not in body
        assert body.index("TaskState::ACTIVE") < body.index(capture) < body.index("procfs_local_wki_hostname()")
        assert "wki_peer_get_hostname(" not in body
    task = (ROOT / "modules/kern/src/platform/sched/task.cpp").read_text()
    for marker, mutation in (("auto Task::wki_execution_identity(", ".submitter_hostname = wki_submitter_hostname"),
                             ("void Task::set_wki_remote_pid(", "wki_remote_pid = remote_pid"),
                             ("void Task::set_wki_proxy_task_id(", "wki_proxy_task_id = proxy_task_id")):
        body = body_after(task, marker)
        assert body.index("path_metadata_lock.lock_irqsave()") < body.index(mutation) < body.index("path_metadata_lock.unlock_irqrestore(")
    peer = (ROOT / "modules/kern/src/net/wki/peer.cpp").read_text()
    copy = body_after(peer, "auto wki_peer_copy_hostname(")
    assert copy.index("peer_lock.lock_irqsave()") < copy.index("wki_peer_find(") < copy.index("if (FOUND) hostname = *source") < copy.index("peer_lock.unlock_irqrestore(")
    assert "if (FOUND) hostname.back()" in copy
    compute = (ROOT / "modules/kern/src/net/wki/remote_compute.cpp").read_text()
    query = body_after(compute, "auto wki_proxy_task_remote_info(")
    assert query.index("wki_execution_identity()") < query.index("wki_submitted_task_remote_info(IDENTITY.proxy_task_id")
    query = body_after(compute, "auto wki_submitted_task_remote_info(")
    assert query.index("s_compute_lock.lock()") < query.index("find_submitted_task_any(task_id)") < query.index("s_compute_lock.unlock()") < query.index("wki_peer_copy_hostname(")
    assert "wki_peer_get_hostname(" not in query
    assert not re.search(r"(?:task|proxy)->wki_proxy_task_id\s*=(?!=)", compute)
    assert not re.search(r"local_task->wki_remote_pid\s*=(?!=)", compute)
    scoped = body_after(compute, "struct ScopedSubmitVfsIdentity")
    constructor = scoped[:scoped.index("[[nodiscard]]")]
    installation = constructor[constructor.index("saved_vfs_rules = std::move("):]
    assert installation.index("path_metadata_lock.lock_irqsave()") < installation.index("saved_submitter = task->wki_submitter_hostname") < installation.index("path_metadata_lock.unlock_irqrestore(") < installation.index("deserialize_task_vfs_rules(")
    restore = body_after(scoped, "~ScopedSubmitVfsIdentity()")
    assert restore.index("path_metadata_lock.lock_irqsave()") < restore.index("task->wki_submitter_hostname = saved_submitter") < restore.index("path_metadata_lock.unlock_irqrestore(")
    wire_copy = body_after(compute, "void fill_task_identity_context(")
    assert "task->credentials()" not in wire_copy
    assert wire_copy.index("credentials.user().real") < wire_copy.index("path_metadata_lock.lock_irqsave()") < wire_copy.index("identity->submitter_hostname.data()") < wire_copy.index("path_metadata_lock.unlock_irqrestore(")


def test_removal_authorization_precedes_namespace_and_disk_effects():
    tmpfs = (ROOT / "modules/kern/src/vfs/fs/tmpfs.cpp").read_text()
    xfs = (ROOT / "modules/kern/src/vfs/fs/xfs/xfs_vfs.cpp").read_text()
    fat = (ROOT / "modules/kern/src/vfs/fs/fat32.cpp").read_text()
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    for operation in ("unlink", "rmdir"):
        local = body_after(tmpfs, f"auto tmpfs_{operation}_lookup(")
        assert local.index("MutexGuard guard(tmpfs_lock)") < local.index("tmpfs_lookup_target_locked(") < local.index("authorize_file_removal(") < local.index("tmpfs_detach_child(")
        assert "metadata_identity_locked(*canonical)" in local
        assert "*hardlink_count_changed = false" not in local
        if operation == "rmdir":
            assert local.index("if (AUTH != 0) return AUTH") < local.index("tmpfs_directory_is_empty(")
        for route in ("path", "lookup"):
            name = f"auto xfs_{operation}_{route}("
            declaration = xfs[xfs.index(name):xfs.index("-> int", xfs.index(name))]
            assert "const ker::mod::security::Snapshot& credentials" in declaration
            body = body_after(xfs, name)
            assert body.index("XfsMetadataGuard metadata_guard") < body.index("authorize_file_removal(credentials,") < body.index("xfs_trans_alloc(") < body.index("xfs_dir_removename(")
            assert "if (rc != 0)" in body[body.index("authorize_file_removal("):body.index("xfs_trans_alloc(")]
            if operation == "rmdir":
                assert body.index("authorize_file_removal(") < body.index("xfs_dir_iterate(")
        body = body_after(fat, f"auto fat32_{operation}_path(")
        assert body.index("fat32_find_dir_entry(") < body.index("authorize_file_removal(") < body.index("if (AUTH != 0) return AUTH") < body.index("remove_fat_entry_locked(")
        assert "{0, 0, 0040755}" in body
        legacy = body_after(core, f"auto vfs_{operation}_resolved_path(")
        assert "tmpfs_walk_path" not in legacy and "tmpfs_detach_child" not in legacy
        assert legacy.index("TmpfsLookupHandle lookup") < legacy.index("tmpfs_acquire_lookup(") < legacy.index(f"tmpfs_{operation}_lookup(credentials,")
    for route in ("auto vfs_unlinkat(", "auto vfs_unlink_resolved_beneath("):
        body = body_after(core, route)
        for fs in ("tmpfs", "xfs"):
            for operation in ("unlink", "rmdir"):
                assert f"{fs}_{operation}_lookup(handle.authorization().credentials," in body
        for operation in ("unlink", "rmdir"):
            assert f"vfs_{operation}_resolved_path(handle.authorization().credentials," in body


def test_directory_paths_are_recovered_from_pinned_identity_before_dispatch():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    lookup = (ROOT / "modules/kern/src/vfs/lookup.cpp").read_text()
    mount = (ROOT / "modules/kern/src/vfs/mount.cpp").read_text()
    tmpfs = (ROOT / "modules/kern/src/vfs/fs/tmpfs.cpp").read_text()
    xfs = (ROOT / "modules/kern/src/vfs/fs/xfs/xfs_vfs.cpp").read_text()
    capture = body_after(core, "auto copy_directory_lookup_path(")
    assert capture.index("if (!namespace_already_locked) publication_guard.emplace(g_vfs_namespace_publication_mutex)") < capture.index("directory->path_metadata_lock.lock_irqsave()") < capture.index("copy_path_string(directory->vfs_path")
    assert "directory->mount_generation == mount_generation" in capture
    assert "directory->vfs_path_namespace_generation == namespace_generation" in capture
    assert capture.index("directory->path_metadata_lock.unlock_irqrestore(IRQF)", capture.index("if (CURRENT)")) < capture.index("copy_mount_path_exact(") < capture.index("tmpfs_directory_path(")
    assert "vfs_file_set_path" not in capture and "vfs_file_clear_path" not in capture
    acquire = body_after(core, "auto vfs_acquire_lookup(")
    assert acquire.index("vfs_get_file_retain(task, dirfd)") < acquire.index("copy_directory_lookup_path(") < acquire.index("bool const DIRECT_DIRFD")
    assert "copy_simple_relative_path_from_base(dirfd_anchor->vfs_path" not in acquire
    assert "IDENTITY_PATH ? directory_path.data() : nullptr" in acquire
    assert "set_dirfd_anchor(candidate, dirfd_anchor, IDENTITY_PATH)" in acquire
    assert acquire.rindex("populate_lookup_path_fields(candidate, path.data(), path_len)") < acquire.index("LookupHandleBuilder::set_mount(candidate,")
    stable = body_after(core, "auto lookup_path_cache_key_still_stable(")
    assert "handle.dirfd_path_revalidated()" in stable
    assert "dirfd_path_revalidated_ = other.dirfd_path_revalidated_" in lookup
    assert "other.dirfd_path_revalidated_ = false" in lookup
    assert "dirfd_path_revalidated_ = false" in body_after(lookup, "void LookupHandle::reset()")
    exact_mount = body_after(mount, "auto copy_mount_path_exact(")
    assert exact_mount.index("mount_lock.lock()") < exact_mount.index("candidate != mount") < exact_mount.index("std::memcpy(out,") < exact_mount.rindex("mount_lock.unlock()")
    assert "directory_path(root, directory, out, capacity, false, nullptr)" in body_after(tmpfs, "auto tmpfs_directory_path(")
    tmp = body_after(tmpfs, "auto directory_path(")
    assert tmp.index("MutexGuard guard(tmpfs_lock)") < tmp.index("node->unlinked") < tmp.index("tmpfs_lookup(node->parent,") < tmp.index("std::memcpy(out,")
    assert "directory_path_at_inode(ctx, anchor, nullptr, out, capacity, nullptr)" in body_after(xfs, "auto xfs_directory_path_at_inode(")
    disk = body_after(xfs, "auto directory_path_at_inode(")
    assert disk.index("XfsMetadataGuard metadata_guard") < disk.index("xfs_inode_read_cached(") < disk.index('xfs_dir_lookup_authoritative(current, "..", 2,') < disk.index("xfs_dir_entry_is_indexed(scan.parent, entry)") < disk.index("scan.entry = *entry")
    assert disk.index("xfs_dir_iterate(parent,") < disk.rindex("xfs_inode_release_metadata_locked(current)") < disk.index("std::memcpy(out,")


def test_mount_admission_precedes_side_effects():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    boundaries = {
        "auto vfs_mount(": ["const char* effective_fstype", "wki_peer_find_by_hostname(", "vfs_acquire_lookup(", "prepare_mount_filesystem("],
        "auto vfs_umount(": ["resolve_task_path_raw(", "stream_invalidate_mount_scope(", "unmount_filesystem_if_dev_id("],
        "auto vfs_pivot_root(": ["snapshot_bounded_path_string(", "wki_remote_vfs_prepare_export_rebuild(", "remap_mounts_for_pivot("],
    }
    for name, effects in boundaries.items():
        body = body_after(core, name)
        assert body.count("->credentials()") == 1
        decision = body.index("authorize_mount_administration(CREDENTIALS)")
        denied = body.index("if (POLICY != 0) return POLICY")
        assert decision < denied
        for effect in effects:
            assert denied < body.index(effect), (name, effect)


def test_mount_target_uses_admission_snapshot_before_preparation():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    mount = body_after(core, "auto vfs_mount(")
    assert "vfs_mkdir(" not in mount
    assert mount.count("vfs_acquire_lookup(") == 2
    assert "&lookup, 3, false, &CREDENTIALS" in mount
    checked = mount.index("if (TARGET != 0) return TARGET")
    for effect in ["wki_peer_find_by_hostname(", "devfs_resolve_block_source(", "probe_block_device_fstype(", "prepare_mount_filesystem("]:
        assert checked < mount.index(effect), effect
    assert "vfs_consume_lookup(" not in mount
    assert mount.count("consume_mount_target(") == 2
    assert mount.count("consume_prepared_mount_target(") == 1
    assert "strip_current_task_root_prefix(" not in mount
    assert mount.count("MountPathKind::RETAINED_PHYSICAL") == 2
    assert "prepare_mount_filesystem(lookup.path()," in mount
    assert "prepare_mount_filesystem(context.lookup->path()," in mount
    assert "consume_mount_target(lookup, *authorization_task, CREDENTIALS)" in mount
    assert "consume_prepared_mount_target(lookup, target, *authorization_task, CREDENTIALS, prepared)" in mount
    retry = body_after(core, "auto consume_prepared_mount_target(")
    assert retry.count("consume_mount_target(lookup, task, credentials, &prepared)") == 2
    assert "&lookup, 3, false, &credentials" in retry
    assert "attempt < 3 && result == -EAGAIN" in retry
    assert "get_current_task" not in retry and "->credentials()" not in retry
    for field in ["task_id", "credentials", "root_hash", "cwd_hash", "root_anchor_identity", "cwd_anchor_identity", "root_identity_view"]:
        assert f"current.{field} != AUTHORIZATION.{field}" in retry
    assert "consume_mount_target(*context.lookup, *context.task, *context.credentials, &prepared)" in mount
    assert ".task = authorization_task, .credentials = &CREDENTIALS" in mount
    consume = body_after(core, "auto consume_mount_target(")
    assert "consume_lookup_locked(handle, prepared != nullptr, consume, &context, &task, &credentials)" in consume
    assert "context{prepared, covered}" in consume
    assert "->credentials()" not in consume and "get_current_task" not in consume
    assert consume.index("validate_mount_target_lookup(target, nullptr)") < consume.index("publish_prepared_mount(")
    assert "publish_prepared_mount(state.prepared, target.path(), state.covered.get())" in consume
    devfs = consume[consume.index("if (target.mount()->fs_type == FSType::DEVFS)"):consume.index("int const VALIDATE")]
    assert devfs.index("authorize_mount_administration(target.authorization().credentials)") < devfs.index("target.backend_parent()")
    assert "return -EAGAIN;" in devfs and "devfs_consume_directory_lookup" in devfs
    assert "publish_devfs_mount_target, &context" in devfs
    for name, start, end in [("auto acquire_local_component_lookup(", "} else if (current.dev &&", "auto* backend = current.dev.release()"),
                             ("auto vfs_acquire_lookup(", "if (mount->fs_type == FSType::DEVFS &&", "auto* backend = new (std::nothrow) ker::vfs::devfs::DevFSNodeRef")]:
        body = body_after(core, name)
        assert "LookupIntent::MOUNT_TARGET" in body[body.index(start):body.index(end)]

    mounts = (ROOT / "modules/kern/src/vfs/mount.cpp").read_text()
    publish = body_after(mounts, "auto publish_prepared_mount_with_identity(")
    assert publish.index("std::strcmp(mount->path, expected_physical_path)") < publish.index("mount_lock.lock()")
    assert "return -EAGAIN;" in publish[:publish.index("mount_lock.lock()")]
    for name in ["auto acquire_local_component_lookup(", "auto vfs_acquire_lookup("]:
        body = body_after(core, name)
        assert "admitted_credentials != nullptr ? *admitted_credentials : task->credentials()" in body
    missing = body_after(core, "auto lookup_intent_allows_missing_final(")
    assert "MOUNT_TARGET" not in missing
    validator = body_after(core, "auto validate_mount_target_lookup(")
    fat = validator[validator.index("FSType::FAT32"):validator.index("vfs_stat_resolved_cache_or_impl(")]
    assert "fat32::fat32_stat(strip_mount_prefix(handle.mount(), handle.path())" in fat
    assert "static_cast<fat32::FAT32MountContext*>(handle.mount()->private_data)" in fat
    assert "metadata_cache" not in fat and "resolve_task" not in fat
    proc = validator[validator.index("FSType::PROCFS"):validator.index("Stat stat{}")]
    assert "static_cast<const File*>(handle.backend_parent())" in proc
    assert "file->is_directory ? 0 : -ENOTDIR" in proc and "return -EAGAIN;" in proc
    for forbidden in ("procfs_lookup", "procfs_fill_stat", "current_task", "vfs_close", "new "):
        assert forbidden not in proc
    walker = body_after(core, "auto acquire_local_component_lookup(")
    binding = walker[walker.index("if (current.proc &&"):walker.index("auto* backend = current.proc.release()")]
    assert "LookupIntent::MOUNT_TARGET" in binding
    fallback = body_after(core, "auto vfs_acquire_lookup(")
    binding = fallback[fallback.index("if (mount->fs_type == FSType::PROCFS &&"):fallback.index("procfs::procfs_lookup_path_as(")]
    assert "LookupIntent::MOUNT_TARGET" in binding


def test_process_restriction_abi_and_publication():
    kernel = (ROOT / "modules/kern/src/abi/callnums/process.h").read_text()
    user = (ROOT / "toolchain/src/mlibc/sysdeps/wos/include/callnums/process.h").read_text()
    for marker in ["enum class CredentialAuthority", "enum class CredentialControl", "enum class ServicePrivilege", "constexpr auto service_privilege_ceiling"]:
        assert re.sub(r"\s+", "", body_after(kernel, marker)) == re.sub(r"\s+", "", body_after(user, marker))
    assert "COUNT = 16" in kernel
    assert "SET_NO_NEW_PRIVILEGES = 38" in kernel and "GET_NO_NEW_PRIVILEGES = 39" in kernel
    assert "GET_AUTHORITY_CEILING = 0x574f5301" in kernel and "DROP_AUTHORITY_CEILING = 0x574f5302" in kernel
    snapshot = (ROOT / "modules/kern/src/platform/security/credentials.hpp").read_text()
    assert "using Authority = ker::abi::process::CredentialAuthority" in snapshot
    process = (ROOT / "modules/kern/src/syscalls_impl/process/process.cpp").read_text()
    prctl = body_after(process, "auto wos_proc_prctl(")
    for option, change in [("SET_NO_NEW_PRIVILEGES", "NO_NEW_PRIVILEGES"), ("DROP_AUTHORITY_CEILING", "DROP_AUTHORITY"), ("SET_SERVICE_PRIVILEGES", "SET_SERVICE_PRIVILEGES")]:
        case = body_after(prctl, f"case static_cast<int>(abi::process::CredentialControl::{option}):")
        assert case.index("-EINVAL") < case.index("SharedVmemPublicationGuard publication_guard") < case.index("change_credentials(")
        assert f"Change::{change}" in case
        for arg in ["arg3", "arg4", "arg5"]: assert f"{arg} != 0" in case
    assert "arg2 != 1" in body_after(prctl, "case static_cast<int>(abi::process::CredentialControl::SET_NO_NEW_PRIVILEGES):")
    assert "arg2 & ~abi::process::CREDENTIAL_AUTHORITY_ALL" in prctl
    assert "arg2 & ~abi::process::SERVICE_PRIVILEGE_ALL" in prctl
    query = body_after(prctl, "case static_cast<int>(abi::process::CredentialControl::GET_SERVICE_PRIVILEGES):")
    assert query.count("task->credentials()") == 1
    for arg in ["arg2", "arg3", "arg4", "arg5"]: assert f"{arg} != 0" in query
    assert "CREDENTIALS.service_privileges()" in query and "CREDENTIALS.service_privileges_locked()" in query
    assert "SERVICE_PRIVILEGES_LOCKED = 1U << 8" in kernel
    authorization = (ROOT / "modules/kern/src/platform/security/authorization.hpp").read_text()
    assert "ServicePrivilege::JOURNAL_READ" in body_after(authorization, "auto authorize_journal_read(")
    assert "ServicePrivilege::NETWORK_ADMIN" in body_after(authorization, "auto authorize_network_administration(")
    assert "allows_service" not in body_after(authorization, "auto authorize_diagnostic(")
    trace = body_after(authorization, "auto may_trace(")
    assert "target.effective_service_privileges() & ~caller.effective_service_privileges()" in trace
    assert "caller.service_privileges_locked() && !target.service_privileges_locked()" in trace


def test_remote_exec_cannot_erase_restrictions():
    compute = (ROOT / "modules/kern/src/net/wki/remote_compute.cpp").read_text()
    authorization = (ROOT / "modules/kern/src/platform/security/authorization.hpp").read_text()
    admission = body_after(authorization, "auto authorize_legacy_remote_exec(")
    assert "restore_legacy_remote_exec_identity(caller.user(), caller.group(), restored)" in admission
    assert "restored == caller" in admission
    mapping = body_after(compute, "auto prepare_submitted_identity(")
    assert mapping.index("restore_legacy_remote_exec_identity(") < mapping.index("auth_policy::map_compute_identity(")
    receiver = body_after(compute, "auto apply_submitted_task_identity(")
    assert "task->credentials() != mapped" in receiver
    assert "initialize_credentials(" not in receiver
    assert "Snapshot::bootstrap_root()" not in receiver and "restore_legacy_remote_exec_identity" not in receiver
    work = body_after(compute, "void handle_task_submit_work(")
    assert work.index("prepare_submitted_identity(") < work.index("load_elf_from_vfs_path(") < work.index("ExecResult const EXEC")
    assert "session.identity.policy, mapped_credentials" in work
    assert "wki_auth_configured_peer_policy" not in compute
    capture = body_after(compute, "auto capture_compute_submit_session_locked(")
    assert ".identity = identity" in capture
    for marker in ["void handle_task_submit(", "void handle_task_cancel("]:
        handler = body_after(compute, marker)
        assert handler.index("capture_compute_peer_identity(") < handler.index("s_compute_lock.lock()")
        assert "rx_channel_generation, IDENTITY)" in handler
    assert "submitted_resources, mapped_credentials, stdio_mask" in work
    for marker in ["auto exec_elf_buffer(", "auto exec_elf_file("]:
        constructor = body_after(compute, marker)
        assert "ProcessReservation process_reservation(resources, credentials.user().real)" in constructor
        assert "initialize_real_uid" not in constructor
    inline = body_after(compute, "auto exec_elf_buffer(")
    assert inline.index("initialize_credentials(*new_task, credentials)") < inline.index("initialize_process_image(")
    file_exec = (ROOT / "modules/kern/src/syscalls_impl/process/exec.cpp").read_text()
    file_wrapper = body_after(file_exec, "auto create_file_backed_process_task(")
    assert "return create_file_backed_process_task_impl(" in file_wrapper
    assert "failure_observations, interpreter_error, nullptr, nullptr, true)" in " ".join(file_wrapper.split())
    file_constructor = body_after(file_exec, "auto create_file_backed_process_task_impl(")
    assert "initialize_real_uid" not in file_constructor
    assert file_constructor.index("initialize_credentials(*new_task, credentials)") < file_constructor.index("initialize_process_image(")
    assert "new_task, submitted_identity, mapped_credentials," in work
    assert "resolve_session_origin(submitted_origins.session, hdr->src_node, SOURCE_BOOT)" in work
    assert "resolve_session_origin(submitted_origins.group, hdr->src_node, SOURCE_BOOT)" in work
    context = body_after(compute, "auto prepare_submitted_resources(")
    assert "!valid_session_origin(origins.session) || !valid_session_origin(origins.group)" in context
    encode = body_after(compute, "void copy_submit_identity_data(")
    assert "JOB.origin.node" in encode and "JOB.group_origin.node" in encode
    assert "sizeof(TaskJobOrigins)" in compute
    placement = body_after(compute, "auto wki_try_remote_spawn(")
    assert placement.index("authorize_legacy_remote_exec(") < placement.index("WkiIpcSubmissionGuard ipc_guard")
    for marker in ["auto wki_task_submit_inline(", "auto wki_task_submit_vfs_ref("]:
        submit = body_after(compute, marker)
        assert submit.index("SUBMIT_CREDENTIALS = filesystem_preparation.credentials()") < submit.index("authorize_legacy_remote_exec(SUBMIT_CREDENTIALS)")
        assert submit.index("return fail_submit(CREDENTIAL_ADMISSION)") < submit.index("wki_peer_capability_negotiated(target_node, WKI_CAP_RESOURCE_LIMITS)")
        assert "FULL_CREDENTIALS ? 0 : ker::mod::security::authorize_legacy_remote_exec(SUBMIT_CREDENTIALS)" in submit
        assert "copy_submit_identity_data(cursor, local_task, resources, SUBMIT_CREDENTIALS, submit_filesystem.root, FULL_CREDENTIALS, exec_signals, ROUTED_HOME, FILESYSTEM)" in " ".join(submit.split())
        assert submit.index("capture_submit_filesystem(filesystem_preparation, submit_filesystem)") < submit.index("build_submit_context_info(")
        assert "cwd = submit_filesystem.cwd;" in submit


def test_explicit_regular_read_keeps_request_authority_separate_from_process_credentials():
    source = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    reader = body_after(source, "auto open_read_file_as_impl(")
    assert "REQUEST_CREDENTIALS = credentials" in reader
    assert "acquire_confined_owner_lookup(path_context, REQUEST_CREDENTIALS, confinement_root, path, LookupIntent::OPEN" in reader
    assert "&lookup, 0, false, &REQUEST_CREDENTIALS" in " ".join(reader.split())
    assert reader.index("lookup_is_beneath_export(lookup, confinement_root)") < reader.index("consume_lookup_locked(lookup")
    owner = body_after(source, "auto vfs_open_read_file_resolved_beneath_as(")
    assert "confinement_root[0] != '/'" in owner and "path[0] != '/'" in owner
    assert "path, false, confinement_root, output" in owner
    assert "consume_lookup_locked(lookup, (flags & (O_CREAT | O_TRUNC)) != 0, consume, &context, &path_context, &REQUEST_CREDENTIALS)" in reader
    assert "initialize_credentials" not in reader and "change_credentials" not in reader
    assert reader.index("authorize_file_access(admitted") < reader.index("tmpfs::tmpfs_open_lookup(admitted")
    assert "xfs::xfs_open_lookup(admitted" in reader and "return -ENOTSUP" in reader
    assert "fat32::fat32_open_path(admitted" in reader and "&result, handle.requires_directory(), context.regular_only)" in " ".join(reader.split())
    mkdir = body_after(source, "auto vfs_mkdir_resolved_path(")
    fat_fallback = mkdir[mkdir.index("if (mount->fs_type == FSType::FAT32)"):mkdir.index("// All mounted filesystem backends")]
    assert "fat32::fat32_stat(fs_path" in fat_fallback and "RESULT == 0 ? -EEXIST : -ENOSYS" in fat_fallback
    assert "vfs_stat(" not in mkdir
    synthetic = body_after(source, "auto synthetic_mkdir_result(")
    assert "devfs::devfs_acquire_path(relative)" in synthetic
    assert "procfs::procfs_lookup_path(relative)" in synthetic
    assert "vfs_stat(" not in synthetic and "devfs_open_path" not in synthetic
    reject = body_after(source, "auto reject_synthetic_mkdir(")
    assert reject.index("synthetic_mkdir_result(") < reject.index("vfs_consume_lookup(")
    assert "vfs_consume_lookup(lookup, false, consume, &result)" in reject
    assert "if (namespace_already_locked) return -EAGAIN" in mkdir
    assert "reject_synthetic_mkdir(lookup)" in body_after(source, "auto vfs_mkdir_resolved_beneath(")
    assert "reject_synthetic_mkdir(lookup, task, &REQUEST_CREDENTIALS)" in body_after(source, "auto vfs_mkdirat(")
    assert "reject_synthetic_mkdir(lookup, &path_context, &REQUEST_CREDENTIALS)" in body_after(source, "auto vfs_mkdir_resolved_beneath_as(")
    assert reject.index("synthetic_mkdir_result(") < reject.index("publication_guard(")
    assert "consume_lookup_locked(lookup, false, consume, &result, request_task, credentials)" in reject
    assert reader.index("authorize_file_access(admitted") < reader.index("fat32::fat32_open_path(admitted")
    ordinary = body_after(source, "auto vfs_consume_lookup(")
    assert "consume_lookup_locked(handle, namespace_mutation, consumer, context)" in ordinary
    validator = body_after(source, "auto lookup_request_context_matches(")
    for token in ["task->pid != authorization.task_id", "authorization.root_hash", "authorization.cwd_hash",
                  "credentials == authorization.credentials"]:
        assert token in validator
    consume = body_after(source, "auto consume_lookup_locked(")
    assert "(request_task == nullptr) != (request_credentials == nullptr)" in consume
    assert "request_task != nullptr && namespace_mutation" not in consume
    general = body_after(source, "auto vfs_open_file_resolved_beneath_as(")
    assert "confinement_root[0] != '/'" in general
    assert "output, flags, mode, false, directory, nullptr, absolute_symlink_root)" in " ".join(general.split())
    assert reader.index("prepared_path.reset(") < reader.index("tmpfs::tmpfs_open_lookup(admitted")
    assert "prepared_path.release()" in reader and "vfs_file_set_path(file" not in reader
    assert reader.index("prepared_path.release()") < reader.index("apply_open_truncation(file, context.flags)")
    assert reader.index("apply_open_truncation(file, context.flags)") < reader.index("context.file = file")
    assert "if (context.regular_only)" in reader
    assert "handle.mount_policy().read_only && open_flags_require_fs_write(context.flags)" in reader
    assert "lookup_request_context_matches(handle, request_task, *request_credentials)" in consume
    assert "lookup_authorization_still_current(handle)" in consume
    assert 'return -EXDEV' in consume


def test_remote_explicit_read_carries_admitted_policy_and_never_downgrades():
    wki = (ROOT / "modules/kern/src/net/wki/wki.cpp").read_text()
    rx = body_after(wki, "void wki_rx(")
    denial = body_after(wki, "void send_policy_denial(")
    for field in (".channel = channel", ".peer_node_id = header.src_node", ".channel_id = header.channel_id", ".generation = generation"):
        assert field in denial
    assert denial.count("wki_send_on_channel_identity(IDENTITY,") == 4
    assert "wki_send_on_channel_generation" not in denial
    assert "response.reserved = static_cast<uint16_t>(header.seq_num)" in denial
    assert rx.index("peer_lifecycle.try_acquire") < rx.index("admitted_identity_policy = authenticated_peer->auth_session.policy")
    assert " ".join(rx.split()).count("admitted_identity_policy, admitted_identity_capabilities);") == 2
    server = (ROOT / "modules/kern/src/net/wki/dev_server.cpp").read_text()
    queue = body_after(server, "auto try_queue_pre_admitted_vfs_op(")
    assert queue.index("op->identity_policy = identity_policy") < queue.index("shard->pending.fetch_add")
    worker = body_after(server, "void run_deferred_vfs_op(")
    assert "op->req_data_len, op->identity_policy" in " ".join(worker.split())
    assert "auth_session" not in worker
    assert "op->identity_capabilities" in worker
    assert queue.index("op->identity_capabilities = identity_capabilities") < queue.index("shard->pending.fetch_add")
    assert "admitted_identity_capabilities = g_wki.capabilities & authenticated_peer->capabilities" in rx
    remote = (ROOT / "modules/kern/src/net/wki/remote_vfs.cpp").read_text()
    owner = body_after(remote, "void handle_vfs_op(")
    opening = owner[owner.index("case OP_VFS_OPEN_READ_AS:"):owner.index("case OP_VFS_READ:")]
    assert opening.index("credential_record::decode") < opening.index("map_vfs_read_identity") < opening.index("prepare_mapped_scalar_path")
    assert "flags != 0 || mode != 0 || data_len != OPEN_PATH_END" in opening
    assert "*worker, mapped_credentials, export_path, full_path.data(), &file" in " ".join(opening.split())
    assert "resp.status = static_cast<int16_t>(open_result)" in opening
    assert "map_vfs_open_identity(identity_policy, supplied, request_flags, mapped_credentials)" in " ".join(opening.split())
    assert "vfs_open_file_resolved_beneath_as(*worker, mapped_credentials" in " ".join(opening.split())
    assert "wki_peer_capability_negotiated" not in opening
    assert "identity_capabilities & REQUIRED_CAPABILITY" in opening
    legacy = owner[owner.index("case OP_VFS_OPEN:"):owner.index("case OP_VFS_OPEN_READ_AS:")]
    assert "reject_request(-ENOTSUP)" in legacy
    assert "vfs_open_file_resolved_beneath(" not in opening
    assert opening.index("HAS_PREFETCH && ker::mod::security::authorize_file_description_access") < opening.index("vfs_open_file_resolved_beneath_as(")
    for op in ["READ", "WRITE", "READDIR", "READDIR_BATCH", "READ_RDMA", "WRITE_RDMA", "READ_BULK"]:
        body = body_after(owner, f"case OP_VFS_{op}:")
        if op in ("WRITE", "WRITE_RDMA"):
            assert body.index("retain_server_write_file(") < body.index("vfs_pwrite_file(") < body.index("vfs_put_file(local_file)")
            assert "local_file->fops->vfs_write" not in body
        else:
            assert body.index("authorize_file_description_access") < body.index("touch_remote_fd(rfd)")
    write_admission = body_after(remote, "auto retain_server_write_file(")
    assert write_admission.index("find_remote_fd(identity, fd)") < write_admission.index("authorize_file_description_access")
    assert write_admission.index("authorize_file_description_access") < write_admission.index("offset < 0") < write_admission.index("file_mount_read_only(*file)")
    assert write_admission.index("file_mount_read_only(*file)") < write_admission.index("vfs_retain_file(file)") < write_admission.index("touch_remote_fd(row)")
    assert write_admission.index("out = file") < write_admission.index("s_vfs_lock.unlock()")
    rdma_write = body_after(owner, "case OP_VFS_WRITE_RDMA:")
    assert "send_rdma_write_err(static_cast<int16_t>(ADMIT))" in rdma_write and "reject_request(" not in rdma_write
    error = body_after(rdma_write, "auto send_rdma_write_err =")
    assert error.index("wki_roce_region_finish_tagged_write") < error.index("wki_dev_server_complete_vfs_write") < error.index("send_simple_resp")
    client = body_after(remote, "auto wki_remote_vfs_open_path(")
    assert client.index("WKI_CAP_VFS_READ_IDENTITY") < client.index("acquire_vfs_proxy_lane")
    assert "regular_only ? OP_VFS_OPEN_READ_AS : OP_VFS_OPEN_AS" in client
    assert "!regular_only && remote_vfs_open_prefetch_capable" in client
    assert client.count("vfs_proxy_send_and_wait(") == 1
    assert "*error = STATUS != 0 ? normalize_proxy_status_for_errno(STATUS) : -EIO" in client
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    remote_read = body_after(core, "auto open_remote_read_file_as(")
    assert "&credentials, &result, true" in " ".join(remote_read.split())
    assert " ".join(core.split()).count("mount->private_data, &CREDENTIALS, &backend_open_result)") == 2
    assert "lookup_request_context_matches" in remote_read and "reservation.attach(file)" in remote_read
    reader = body_after(core, "auto open_read_file_as_impl(")
    assert reader.index("if (confinement_root != nullptr) return -EPERM") < reader.index("open_remote_read_file_as(")
    task_source = (ROOT / "modules/kern/src/platform/sched/task.cpp").read_text()
    interpreter = body_after(task_source, "auto read_boot_file_fully(")
    assert "CREDENTIALS = image_task.credentials()" in interpreter
    assert interpreter.index("vfs_open_read_file_as(path_context, CREDENTIALS") < interpreter.index("boot_file_cache_lookup_copy(")
    assert "vfs_open_file(" not in interpreter
    assert "current_task->credentials()" not in interpreter


def test_remote_elf_source_requires_mapped_read_before_cache_or_construction():
    compute = (ROOT / "modules/kern/src/net/wki/remote_compute.cpp").read_text()
    loader = body_after(compute, "auto load_elf_from_vfs_path(")
    assert "REQUEST_CREDENTIALS = credentials" in loader
    assert "vfs_open_read_file_as(*CURRENT_TASK, REQUEST_CREDENTIALS, resolved_path" in loader
    assert loader.index("int const OPEN_ERROR = admit_source()") < loader.index("find_shared_elf_cache_locked(")
    assert loader.index("int const OPEN_ERROR = admit_source()") < loader.index("find_shared_elf_cache_entry_locked(")
    assert "int const RETRY_OPEN_ERROR = admit_source()" in loader
    assert "source.reset(opened)" in loader and "vfs_put_file(file)" in loader
    assert "vfs_pread_file(source.get()" in loader and "vfs_fstat_file(source.get()" in loader
    assert "result.file = source.release()" in loader
    for ambient in ("vfs_open(", "vfs_open_file(", "vfs_stat(", "vfs_pread(", "vfs_get_file_retain(", "credentials()"):
        assert ambient not in loader
    assert "equivalent_local_file_for_wki_ref" not in compute
    assert "RETRY_OPEN_ERROR == -EACCES" in loader and "RETRY_OPEN_ERROR == -ENOTSUP" in loader
    work = body_after(compute, "void handle_task_submit_work(")
    assert work.index("ScopedSubmitVfsIdentity submit_vfs_identity(") < work.index("load_elf_from_vfs_path(")
    assert work.index("!submit_vfs_identity.policy_valid()") < work.index("load_elf_from_vfs_path(")
    assert " ".join(work.split()).count("session, mapped_credentials, deadline_us)") == 2
    assert work.count("load_elf_from_vfs_path(*current_task,") == 2
    assert work.count("reject_reason = vfs_result.reject_reason") == 2


def test_executable_cache_candidates_require_retained_source_content():
    task = (ROOT / "modules/kern/src/platform/sched/task.cpp").read_text()
    boot = body_after(task, "auto read_boot_file_fully(")
    assert boot.index("boot_file_cache_lookup_copy(") < boot.index("content_matches(") < boot.index("*out_buf = cached")
    assert "vfs_pread_file(file, bytes, count" in boot and "delete[] cached" in boot
    compute = (ROOT / "modules/kern/src/net/wki/remote_compute.cpp").read_text()
    loader = body_after(compute, "auto load_elf_from_vfs_path(")
    candidate = loader[loader.index("if (auto* cached = find_shared_elf_cache_locked("):loader.index("auto* inflight =", loader.index("if (auto* cached = find_shared_elf_cache_locked("))]
    assert candidate.index("cached->refcount++") < candidate.index("s_compute_lock.unlock()") < candidate.index("content_matches(")
    assert candidate.index("content_matches(") < candidate.index("result.buffer = CACHED_BUFFER")
    assert "request_is_current)" in " ".join(candidate.split())
    assert "find_shared_elf_cache_by_buffer_locked(CACHED_BUFFER)" in candidate
    assert candidate.index("stale->valid = false") < candidate.index("release_cached_elf_buffer(CACHED_BUFFER)")
    assert loader.count("find_shared_elf_cache_locked(") == 1
    assert "result.buffer = inflight->buffer" not in loader
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    for marker in ("auto open_remote_read_file_as(", "auto open_read_file_as_impl("):
        assert "O_NOTIFY_CACHE_CHANGE | O_NO_CACHE" in body_after(core, marker)



def test_owner_confinement_is_enforced_during_component_traversal():
    source = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    helper = body_after(source, "auto acquire_confined_owner_lookup(")
    assert "&credentials, false, true, confinement_root" in " ".join(helper.split())
    assert "RESULT == RESOLVE_FAST_PATH_DECLINED ? -ENOTSUP : RESULT" in helper
    walker = body_after(source, "auto acquire_local_component_lookup(")
    assert "walk_pathname += std::strlen(root.data())" in walker
    boundary = "DOTDOT && confinement_root != nullptr && std::strcmp(path.data(), root.data()) == 0"
    assert walker.index("local_walk_search(current, authorization.credentials)") < walker.index(boundary)
    links = walker[walker.index("if (confinement_root != nullptr && link[0] == '/')"):]
    assert links.index("path_prefix_matches(link, absolute_symlink_root, VISIBLE_LENGTH)") < links.index("physical_export_root.data()")
    assert links.index("physical_export_root.data()") < links.index("path_prefix_matches(link, root.data(), ROOT_LEN)")
    assert links.index("if (!path_prefix_matches(link, root.data(), ROOT_LEN)) return -EPERM") < links.index("local_walk_append(link,")
    assert walker.index("physical_export_root = root") < walker.index("auto admit_retained_view")
    assert helper.index("normalize_owner_symlink_root(") < helper.index("acquire_local_component_lookup(")
    normalize = body_after(source, "auto normalize_owner_symlink_root(")
    assert "output[START + 1] == '.'" in normalize
    assert normalize.index("output[START + 1] == '.'") < normalize.index("canonicalize_path(")
    acquire = body_after(source, "auto vfs_acquire_directory_view_lookup(")
    assert "(export_path == nullptr) != (export_name == nullptr)" in acquire
    assert acquire.index("normalize_owner_symlink_root(export_name") < acquire.index("MutexGuard publication_guard")
    assert "normalize_owner_symlink_root(export_path, physical_origin)" in acquire
    assert "export_path != nullptr ? physical_origin.data() : nullptr" in acquire
    assert walker.index("copy_path_string(absolute_symlink_origin") < walker.index("auto admit_retained_view")
    reader = body_after(source, "auto open_read_file_as_impl(")
    assert "directory, absolute_symlink_root)" in reader
    for name in ("auto open_read_file_as_impl(", "auto vfs_stat_resolved_beneath_as(", "auto vfs_readlink_resolved_beneath_as("):
        body = body_after(source, name)
        assert "acquire_confined_owner_lookup(" in body
        assert "REQUEST_CREDENTIALS" in body and "lookup_is_beneath_export" in body


    for operation in ("mkdir", "symlink", "unlink", "rename", "chmod", "chown", "utimens"):
        mutation = body_after(source, "auto vfs_" + operation + "_resolved_beneath_as(")
        count = 2 if operation == "rename" else 1
        assert mutation.count("acquire_confined_owner_lookup(") == count
        assert mutation.count("absolute_symlink_root)") == count
        assert mutation.index("REQUEST_CREDENTIALS = credentials") < mutation.index("for (unsigned attempt")
        assert "consume_lookup_pair_locked" in mutation if operation == "rename" else "consume_lookup_locked" in mutation
    native = (ROOT / "modules/kern/src/test/credential_ktest.cpp").read_text()
    assert "TmpfsMappedMutationsUseCapturedSymlinkNamespace" in native
    assert "XfsMappedMutationsUseCapturedSymlinkNamespace" in native


def test_explicit_owner_stat_stages_metadata_under_one_snapshot():
    source = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    stat = body_after(source, "auto vfs_stat_resolved_beneath_as(")
    assert "REQUEST_CREDENTIALS = credentials" in stat
    assert "acquire_confined_owner_lookup(path_context, REQUEST_CREDENTIALS, confinement_root, path, LookupIntent::STAT" in " ".join(stat.split())
    assert stat.index("lookup_is_beneath_export") < stat.index("consume_lookup_locked(") < stat.index("*statbuf = candidate")
    assert "&candidate, &path_context, &REQUEST_CREDENTIALS" in stat
    retained = body_after(source, "auto stat_owner_lookup(")
    assert "stat_owner_lookup(handle, static_cast<Stat*>(opaque))" in stat
    for backend in ("tmpfs_stat_lookup", "xfs_stat_lookup", "fat32_stat", "devfs_stat_node"):
        assert backend in retained
    for forbidden in ("current_metadata_credentials", "task->credentials", "get_current_task", "metadata_cache", "vfs_stat_resolved_cache", "procfs_lookup_path", "vfs_open", "vfs_alloc_fd"):
        assert forbidden not in stat + retained
    assert "return -EPERM" in stat and "-ENOTSUP" in retained
    assert "handle.requires_directory()" in retained
    acquire = body_after(source, "auto vfs_acquire_lookup(")
    assert "PHYSICAL_OWNER_PATH = already_resolved && admitted_credentials != nullptr" in acquire
    assert "if (!already_resolved || PHYSICAL_OWNER_PATH)" in acquire
    assert "&REQUEST_CREDENTIALS, false, PHYSICAL_OWNER_PATH" in acquire
    assert "if (PHYSICAL_OWNER_PATH && WALK == RESOLVE_FAST_PATH_DECLINED) return -ENOTSUP" in acquire
    walker = body_after(source, "auto acquire_local_component_lookup(")
    assert walker.index("authorization.root_hash =") < walker.index("if (physical_path) {")
    assert "LocalWalkEpoch EPOCH{NAMESPACE, MOUNTS, serialized, physical_path, ker::mod::sched::task::process_pid(*task), task, confined_mount, &references.membership}" in " ".join(walker.split())
    assert "local_walk_search(current, authorization.credentials)" in walker
    bind = body_after(source, "auto local_walk_bind(")
    assert "epoch.physical_path ? -EPERM : RESOLVE_FAST_PATH_DECLINED" in bind
    assert bind.index("epoch.physical_path && mount->fs_type == FSType::PROCFS") < bind.index("local_walk_read_stat(")
    prefix = body_after(bind, "if (mount == nullptr)")
    assert "if (!epoch.physical_path) return -ENOENT" in prefix
    assert prefix.index("if (!epoch.current())") < prefix.index("if (!find_first_mount_child(path))") < prefix.index("fill_synthetic_mount_dir_stat(")


def test_remote_fstat_uses_retained_descriptor_and_staged_output():
    source = (ROOT / "modules/kern/src/net/wki/remote_vfs.cpp").read_text()
    client = body_after(source, "auto wki_remote_vfs_fstat(")
    assert client.index("WKI_CAP_VFS_FD_METADATA") < client.index("flush_write_behind(ctx)") < client.index("OP_VFS_FSTAT")
    assert client.index("response_length != sizeof(candidate)") < client.index("*statbuf = candidate")
    assert "candidate.st_dev = file->mount_dev_id" in client
    for forbidden in ("vfs_path", "find_mount_point", "remote_vfs_stat_on_proxy", "remote_vfs_try_copy_cached_stat", "f->pos", "file->pos", "credentials"):
        assert forbidden not in client
    owner = body_after(source, "case OP_VFS_FSTAT: {")
    assert owner.index("WKI_CAP_VFS_FD_METADATA") < owner.index("data_len != sizeof(int32_t)") < owner.index("find_remote_fd(channel_identity, fd_id)")
    assert owner.index("if (rfd == nullptr || rfd->file == nullptr)") < owner.index("touch_remote_fd(rfd)")
    assert owner.index("auto* local_file = rfd->file") < owner.rindex("s_vfs_lock.unlock()") < owner.index("vfs_fstat_file(local_file, &stat)")
    assert "authorize_file_description_access" not in owner and "vfs_stat_resolved" not in owner
    reply = body_after(source, "void handle_vfs_op_resp(")
    assert "wki_vfs_fstat_response_valid(resp->status, RESP_DATA_LEN)" in reply
    assert reply.index("if (INVALID_RESPONSE)") < reply.index("memcpy(state->op_resp_buf")
    rejection = reply[reply.index("bool const INVALID_RESPONSE"):reply.index("if (INVALID_RESPONSE)")]
    assert all(reason in rejection for reason in ("INVALID_STAT", "INVALID_BATCH", "INVALID_PATH", "INVALID_EXEC", "INVALID_FEATURES", "INVALID_DIRECTORY", "INVALID_GRANT", "INVALID_OPEN"))
    assert "INVALID_STAT ||" in reply and "if (!INVALID_RESPONSE" in reply and "state->op_status = -EPROTO" in reply
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    assert "file->fs_type == FSType::REMOTE" in body_after(core, "auto file_stat_snapshot_cacheable(")
    stat = body_after(core, "auto vfs_fstat_file(")
    assert "file->fs_type != FSType::XFS && file->fs_type != FSType::REMOTE" in stat
    remote = stat[stat.index("case FSType::REMOTE:"):stat.index("case FSType::PROCFS:")]
    assert "return ker::net::wki::wki_remote_vfs_fstat(file, statbuf)" in remote
    assert "finish_stat" not in remote and "st_mode" not in remote
    compute = (ROOT / "modules/kern/src/net/wki/remote_compute.cpp").read_text()
    loader = body_after(compute, "auto load_elf_from_vfs_path(")
    assert "result.reject_reason = source_rejection(STAT_ERROR)" in loader
    for name in ("POST_OPEN_STAT_ERROR", "RETRY_STAT_ERROR", "POST_READ_STAT_ERROR"):
        assert f"result.reject_reason = source_metadata_rejection({name})" in loader
    assert "RETRY_STAT_ERROR == -ENOTSUP" in loader


def test_remote_path_stat_preserves_caller_identity_and_no_legacy_fallback():
    source = (ROOT / "modules/kern/src/net/wki/remote_vfs.cpp").read_text()
    client = body_after(source, "auto remote_vfs_stat_on_proxy(")
    assert client.index("WKI_CAP_VFS_STAT_IDENTITY") < client.index("credential_record::encode") < client.index("OP_VFS_STAT_AS")
    assert client.index("response_length != sizeof(candidate)") < client.index("*statbuf = candidate")
    owner = body_after(source, "case OP_VFS_STAT_AS: {")
    assert owner.index("WKI_CAP_VFS_STAT_IDENTITY") < owner.index("credential_record::decode") < owner.index("map_vfs_read_identity") < owner.index("vfs_stat_resolved_beneath_as")
    assert "WKI_VFS_STAT_REQUIRE_DIRECTORY" in owner and "WKI_VFS_STAT_FOLLOW" in owner
    handler = body_after(source, "void handle_vfs_op(")
    legacy = handler.split("case OP_VFS_STAT:", 1)[1].split("case OP_VFS_STAT_AS:", 1)[0]
    assert "reject_request(-ENOTSUP)" in legacy and "vfs_stat_resolved" not in legacy
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    for name in ("vfs_statat", "vfs_faccessat"):
        body = body_after(core, "auto " + name + "(")
        assert body.count("task->credentials()") == 1
        assert "false, &REQUEST_CREDENTIALS" in body
        assert body.index("wki_remote_vfs_stat(") < body.index("metadata_cache_lookup(")
        assert "handle.authorization().credentials" in body
    resolver = body_after(core, "auto resolve_symlinks(")
    assert resolver.index("stop_at_remote_mount && mount != nullptr") < resolver.index("resolve_prefix_symlink_once(") < resolver.index("wki_remote_vfs_readlink_path(")
    acquire = " ".join(body_after(core, "auto vfs_acquire_lookup(").split())
    assert "nullptr, task, intent == LookupIntent::OPEN, intent == LookupIntent::OPEN || intent == LookupIntent::STAT || intent == LookupIntent::ACCESS" in acquire
    server = (ROOT / "modules/kern/src/net/wki/dev_server.cpp").read_text()
    preflight = body_after(server, "auto vfs_metadata_batch_preflight(")
    assert "header.version == VFS_METADATA_BATCH_VERSION" in preflight and "return -ENOTSUP" in preflight
    assert preflight.index("credential_record::decode") < preflight.index("auto parse_path")
    batch = body_after(server, "void handle_vfs_metadata_batch_op(")
    assert batch.index("vfs_metadata_batch_preflight") < batch.index("map_vfs_read_identity") < batch.index("vfs_stat_resolved_beneath_as")
    assert "vfs_stat_resolved_beneath(" not in batch
    construction = body_after(core, "auto vfs_metadata_batch(")
    assert construction.count("task->credentials()") == 1
    assert "path_mount_ref = retain_mount_point(lookup.mount())" in construction
    assert "count, results, &mutation_request_attempted, &REQUEST_CREDENTIALS" in " ".join(construction.split())
    sender = body_after(source, "auto wki_remote_vfs_metadata_batch(")
    assert sender.count("credential_record::encode(*credentials)") == 1
    assert sender.index("credential_record::encode(*credentials)") < sender.index("while (first < count)")
    assert "response[OFFSET + byte] != 0" in sender


def test_routed_lookup_preserves_admitted_snapshot_across_resolution():
    source = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    lookup = body_after(source, "auto vfs_acquire_lookup(")
    assert lookup.count("task->credentials()") == 1
    assert lookup.index("REQUEST_CREDENTIALS =") < lookup.index("for (unsigned restart")
    assert "apply_task_route, out, &REQUEST_CREDENTIALS" in lookup
    assert ".credentials = REQUEST_CREDENTIALS" in lookup
    resolution = lookup[lookup.index("result = resolve_symlinks("):]
    assert "&REQUEST_CREDENTIALS);" in resolution[:resolution.index("if (result < 0)")]
    resolver = body_after(source, "auto resolve_symlinks(")
    assert "admitted_credentials == nullptr && effective_task_policy != nullptr" in resolver
    assert resolver.count("effective_task_policy->credentials()") == 1
    assert "preserve_procfs_fd_links, admitted_credentials)" in " ".join(resolver.split())
    remote = resolver[resolver.index("if (admitted_credentials == nullptr) return -ESRCH;"):]
    assert "*admitted_credentials" in remote[:remote.index("if (LINK_LEN")]


def test_foreign_local_routes_keep_original_ancestors_and_checked_handoff():
    source = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    walker = body_after(source, "auto acquire_local_component_lookup(")
    assert walker.index("route_local_walk_path(") < walker.index("unsigned symlinks")
    assert "local_walk_search(current, authorization.credentials)" in walker
    assert "auto publish_remote =" in walker
    handoff = walker[walker.index("auto publish_remote ="):walker.index("unsigned symlinks")]
    for token in ("if (serialized)", "EPOCH.current()", "lookup_request_context_matches", "set_policy(candidate, intent, follow_policy, needs_directory, authorization)"):
        assert token in handoff
    links = walker[walker.index("result = local_walk_append(link,"):]
    assert "route_local_walk_path(" in links
    assert "publish_remote(routed.data(), std::move(destination))" in links
    assert "pending = replacement" in links
    route = body_after(source, "auto route_local_walk_path(")
    assert route.index("canonicalize_path(visible.data()") < route.index("apply_task_vfs_route(")
    assert "unchanged = std::strcmp(physical.data(), raw_route.data()) == 0" in route
    assert "local_walk_append(ROOT_LEN > 1 ? root :" in route


def test_local_submitter_rules_do_not_bypass_native_ancestor_walk():
    source = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    predicate = body_after(source, "auto task_has_common_local_vfs_routing(")
    assert "wki_vfs_rules" not in predicate
    assert "task->wki_submitter_hostname" in predicate and "g_wki.local_hostname" in predicate
    walker = body_after(source, "auto acquire_local_component_lookup(")
    assert "ROUTED_LOCAL = apply_task_route && !task_has_common_local_vfs_routing(task)" in walker
    assert "route_local_walk_path(task, root.data(), path.data(), pending.data(), routed, unchanged)" in " ".join(walker.split())
    assert "if (!REMOTE_COMPONENTS || serialized) return RESOLVE_FAST_PATH_DECLINED" in walker
    assert "path_is_wki_prefix(pathname)" in walker
    route = body_after(source, "auto apply_task_vfs_route(")
    assert "WkiVfsRoute::HOST" in route and "task_submitter_hostname(task)" in route
    assert "std::strcmp(submitter, ker::net::wki::g_wki.local_hostname.data()) == 0" in route


def test_local_readlink_checks_retained_directory_and_devfs_before_fallback():
    source = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    body = body_after(source, "auto vfs_readlinkat(")
    assert body.count("task->credentials()") == 1
    assert body.index("wki_remote_vfs_readlink_path") < body.index("stat_owner_lookup(handle, &metadata)") < body.index("tmpfs_readlink_lookup")
    assert "return STAT != 0 ? STAT : -EINVAL" in body
    assert body.index("devfs::DevFSNodeRef") < body.index("readlink_resolved(handle.path()")
    assert "->readlink(args.buffer, args.size)" in body
    fallback = body_after(source, "auto vfs_acquire_lookup(")
    assert "(intent == LookupIntent::METADATA || intent == LookupIntent::CHANGE_MODE || intent == LookupIntent::CHANGE_OWNER || (intent == LookupIntent::CHANGE_TIMES || intent == LookupIntent::XATTR) || intent == LookupIntent::READLINK || intent == LookupIntent::MOUNT_TARGET || intent == LookupIntent::MOUNT_SOURCE)" in " ".join(fallback.split())


def test_explicit_owner_readlink_stages_bytes_under_one_snapshot():
    source = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    body = body_after(source, "auto vfs_readlink_resolved_beneath_as(")
    assert "REQUEST_CREDENTIALS = credentials" in body
    assert "bufsize > MAX_PATH_LEN" in body and "return -EMSGSIZE" in body
    assert body.index("acquire_confined_owner_lookup(") < body.index("MutexGuard publication_guard") < body.index("lookup_is_beneath_export") < body.index("consume_lookup_locked") < body.index("std::memcpy(buf,")
    assert "&path_context, &REQUEST_CREDENTIALS" in body
    assert "LookupIntent::READLINK, LookupFollowPolicy::NOFOLLOW_FINAL" in " ".join(body.split())
    assert body.index("if (handle.requires_directory())") < body.index("stat_owner_lookup(handle, &metadata)") < body.index("tmpfs_readlink_lookup")
    assert "return STAT != 0 ? STAT : -EINVAL" in body
    for token in ("tmpfs_readlink_lookup", "xfs_readlink_lookup", "devfs::DevFSNodeRef", "result = -ENOSYS"):
        assert token in body
    for forbidden in ("get_current_task", "bootstrap_root", "vfs_readlink_resolved(", "cache_lookup", "vfs_open"):
        assert forbidden not in body


def test_remote_readlink_uses_mapped_snapshot_without_cache_or_fallback():
    source = (ROOT / "modules/kern/src/net/wki/remote_vfs.cpp").read_text()
    client = body_after(source, "auto wki_remote_vfs_readlink_path(")
    assert client.index("REQUEST_CREDENTIALS = credentials") < client.index("WKI_CAP_VFS_PATH_IDENTITY") < client.index("credential_record::encode(") < client.index("vfs_proxy_read_with_retry(")
    assert client.index("wki_vfs_path_response_valid(") < client.index("std::memcpy(buf,")
    for forbidden in ("try_readlink_cache_lookup", "cache_readlink_result", "readlink_unsupported", "get_current_task", "OP_VFS_READLINK"):
        assert forbidden not in client
    owner = body_after(source, "case OP_VFS_PATH_AS: {")
    assert owner.index("WKI_CAP_VFS_PATH_IDENTITY") < owner.index("credential_record::decode(") < owner.index("map_vfs_read_identity(") < owner.index("vfs_readlink_resolved_beneath_as(")
    assert "*task, mapped, export_path" in " ".join(owner.split())
    assert "WKI_VFS_PATH_REQUIRE_DIRECTORY" in owner
    assert "vfs_readlink_resolved_beneath(" not in owner and "task->credentials()" not in owner
    handler = body_after(source, "void handle_vfs_op(")
    legacy = handler.split("case OP_VFS_READLINK:", 1)[1].split("case OP_VFS_PATH_AS:", 1)[0]
    assert "reject_request(-ENOTSUP)" in legacy and "vfs_readlink" not in legacy
    reply = body_after(source, "void handle_vfs_op_resp(")
    assert reply.index("bool const INVALID_PATH") < reply.index("memcpy(state->op_resp_buf")
    assert "INVALID_STAT || INVALID_BATCH || INVALID_PATH" in reply
    assert "if (!INVALID_RESPONSE" in reply
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    public = body_after(core, "auto vfs_readlinkat(")
    assert public.count("task->credentials()") == 1
    assert "false, &REQUEST_CREDENTIALS" in " ".join(public.split())
    assert "handle.authorization().credentials, handle.requires_directory()" in public
    assert public.index("wki_remote_vfs_readlink_path(") < public.index("tmpfs_readlink_lookup(")
    resolved = body_after(core, "auto readlink_resolved_on_mount(")
    assert resolved.index("wki_remote_vfs_readlink_path(") < resolved.index("symlink_cache_lookup(")
    assert "if (task == nullptr) return -ESRCH" in resolved
    resolver = body_after(core, "auto resolve_symlinks(")
    assert "captured_credentials.emplace(effective_task_policy->credentials())" in resolver
    assert "LINK_LEN != -ENOTDIR" in resolver and "return static_cast<int>(LINK_LEN)" in resolver



def test_truncate_checks_retained_write_grant_before_effect():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    helper = body_after(core, "auto vfs_ftruncate_file(")
    assert helper.index("authorize_file_description_access") < helper.index("file->fops->vfs_truncate(file, length)") < helper.index("cache_notify_file_data_changed_impl")
    assert "file_mount_read_only(*file)" in helper and "if (ACCESS != 0) return ACCESS" in helper
    assert "credentials()" not in helper and "vfs_open" not in helper
    syscall = body_after(core, "auto vfs_ftruncate(")
    assert syscall.index("vfs_get_file_retain") < syscall.index("vfs_ftruncate_file") < syscall.index("vfs_put_file")
    remote = (ROOT / "modules/kern/src/net/wki/remote_vfs.cpp").read_text()
    client = body_after(remote, "auto remote_vfs_truncate(")
    assert client.index("authorize_file_description_access") < client.index("FileGrowth growth") < client.index("remote_vfs_truncate_impl")
    owner = remote[remote.index("case OP_VFS_TRUNCATE: {"):remote.index("case OP_VFS_SEEK_END: {")]
    assert owner.index("data_len != sizeof(int32_t) + sizeof(int64_t)") < owner.index("find_remote_fd(channel_identity")
    assert owner.index("vfs_retain_file") < owner.index("s_vfs_lock.unlock()") < owner.index("vfs_ftruncate_file") < owner.index("vfs_put_file")
    assert "->fops->vfs_truncate" not in owner
    opened = body_after(core, "auto apply_open_truncation(")
    assert opened.index("wki_remote_vfs_get_fops()) return 0") < opened.index("f->fops->vfs_truncate")


def test_mutation_batches_map_one_snapshot_before_any_item():
    source = (ROOT / "modules/kern/src/net/wki/dev_server.cpp").read_text()
    execute = body_after(source, "void handle_vfs_metadata_batch_op(")
    assert execute.index("vfs_metadata_batch_preflight") < execute.index("map_vfs_mutation_identity") < execute.index("for (size_t index")
    for name in ("vfs_open_file_resolved_beneath_as", "vfs_unlink_resolved_beneath_as", "vfs_rename_resolved_beneath_as"):
        assert name in execute
    for name in ("vfs_open_file_resolved_beneath(", "vfs_unlink_resolved_beneath(", "vfs_rename_resolved_beneath("):
        assert name not in execute
    assert "file != nullptr ?" not in execute
    core = body_after((ROOT / "modules/kern/src/vfs/core.cpp").read_text(), "auto vfs_metadata_batch(")
    assert "resolve_dirfd_task_path_raw" not in core
    assert "&REQUEST_CREDENTIALS" in core
    for intent in ("OPEN", "REMOVE", "RENAME_SOURCE", "RENAME_TARGET"):
        assert "LookupIntent::" + intent in core


def test_mapped_components_require_negotiation_and_preserve_raw_ancestors():
    source = (ROOT / "modules/kern/src/net/wki/remote_vfs.cpp").read_text()
    status = body_after(source, "auto mapped_wire_path_status(")
    assert "relative_wire_path_has_safe_components(path, path_len, true)" in status
    assert "? 0 : -ENOTSUP" in status
    prepare = body_after(source, "auto prepare_mapped_scalar_path(")
    assert prepare.index("mapped_wire_path_status(") < prepare.index("build_full_path(")
    assert "canonicalize" not in prepare
    legacy = body_after(source, "auto prepare_legacy_scalar_path(")
    assert "relative_wire_path_has_safe_components(relative_path, relative_path_len)" in legacy
    handler = body_after(source, "void handle_vfs_op(")
    assert "COMPONENT_PATHS = (identity_capabilities & WKI_CAP_VFS_COMPONENT_PATHS) != 0" in handler
    assert handler.count("prepare_mapped_scalar_path(") == 10
    assert "wki_peer_capability_negotiated" not in handler
    for name in ("auto wki_remote_vfs_open_path(", "auto remote_vfs_stat_on_proxy(", "auto wki_remote_vfs_readlink_path("):
        client = body_after(source, name)
        assert "mapped_wire_path_status(" in client and "WKI_CAP_VFS_COMPONENT_PATHS" in client
        assert "PATH_STATUS != 0" in client
    server = (ROOT / "modules/kern/src/net/wki/dev_server.cpp").read_text()
    preflight = body_after(server, "auto vfs_metadata_batch_preflight(")
    assert "export_name, component_paths)" in preflight
    execute = body_after(server, "void handle_vfs_metadata_batch_op(")
    assert "(identity_capabilities & WKI_CAP_VFS_COMPONENT_PATHS) != 0" in execute
    route = body_after((ROOT / "modules/kern/src/vfs/core.cpp").read_text(), "auto route_local_walk_path(")
    assert "apply_task_vfs_route(task, physical.data(), raw_route.data()" in route
    assert route.index("std::strcmp(normalized_route.data(), routed.data()) != 0) return -ENOTSUP") < route.index("routed = raw_route")
    wire = (ROOT / "modules/kern/src/net/wki/wire.hpp").read_text()
    assert "WKI_CAP_VFS_COMPONENT_PATHS = 0x8000" in wire
    startup = (ROOT / "modules/kern/src/net/wki/wki.cpp").read_text()
    assert '"wki.vfs_component_paths_off"' in startup



def test_initial_mapped_routes_do_not_fall_back_to_normalized_text():
    source = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    walker = body_after(source, "auto acquire_local_component_lookup(")
    assert "REMOTE_COMPONENTS = !physical_path" in " ".join(walker.split())
    assert "intent == LookupIntent::CHANGE_MODE" in walker
    for intent in ("OPEN", "STAT", "ACCESS", "READLINK"):
        assert "intent == LookupIntent::" + intent in walker
    assert "path_is_wki_prefix(pathname) && !REMOTE_COMPONENTS" in walker
    assert "physical_path ? -EPERM : RESOLVE_FAST_PATH_DECLINED" in walker
    assert walker.index("auto publish_remote =") < walker.index("bool route_restarted")
    initial = walker[walker.index("bool route_restarted"):walker.index("unsigned symlinks")]
    assert "maybe_ensure_wki_host_root_mount_for_task(task, routed.data())" in initial
    assert "publish_remote(routed.data(), std::move(destination))" in initial
    assert initial.index("publish_remote(routed.data(), std::move(destination))") < initial.index("if (!unchanged)")
    assert "route_restarted ? nullptr : anchor.get()" in initial
    assert "retained_cwd = nullptr" in initial and "path = root" in initial
    assert "ROUTED_LOCAL || REMOTE_COMPONENTS" in initial
    links = walker[walker.index("result = local_walk_append(link,"):]
    assert "ROUTED_LOCAL || (REMOTE_COMPONENTS && apply_task_route)" in links
    route = body_after(source, "auto route_local_walk_path(")
    assert route.index("physical[written] =") < route.index("apply_task_vfs_route(")
    assert "routed_visible = ROOT_LEN > 1 ? raw_route.data() + ROOT_LEN : raw_route.data()" in route
    assert "local_walk_append(root, normalized_route.data(), canonical)" in route
    assert "unchanged = std::strcmp(physical.data(), raw_route.data()) == 0" in route


def test_remote_alias_handoff_selects_complete_raw_export_and_preserves_grants():
    source = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    walker = body_after(source, "auto acquire_local_component_lookup(")
    handoff = walker[walker.index("auto publish_remote ="):walker.index("bool route_restarted")]
    assert "copy_path_string(remote_path, full_path.data(), full_path.size())" in handoff
    assert "auto selected = find_mount_point(full_path.data(), written)" in handoff
    assert "selected->fs_type == FSType::REMOTE" in handoff
    assert "mount = std::move(selected)" in handoff
    assert handoff.index("find_mount_point(") < handoff.index("MutexGuard publication_guard")
    assert handoff.index("EPOCH.current()") < handoff.index("set_mount(candidate")
    assert "populate_lookup_path_fields(candidate, full_path.data(), written)" in handoff
    assert "needs_directory, authorization" in handoff
    assert "lookup_request_context_matches(candidate, task, authorization.credentials)" in handoff
    for forbidden in ("canonicalize_path", "task->credentials()", "wki_remote_vfs_", "maybe_ensure_"):
        assert forbidden not in handoff
    grant = walker[walker.index("if (!physical_path && anchor->fs_type == FSType::REMOTE"):walker.index("bool needs_directory")]
    assert "retain_mount_point(anchor->mount_owner)" in grant
    assert "set_remote_relative_path(candidate, pathname" in grant
    assert "set_dirfd_anchor(candidate, anchor.release())" in grant
    assert "publish_remote" not in grant


def test_owner_directory_grant_uses_retained_inode_and_admitted_snapshot():
    source = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    walker = body_after(source, "auto acquire_local_component_lookup(")
    assert "vfs_retain_file(owner_directory)" in walker
    assert "anchor.reset(owner_directory)" in walker
    assert "copy_directory_lookup_path(anchor.get(), NAMESPACE, MOUNTS, path, serialized)" in walker
    assert "owner_directory != nullptr ? -EPERM : -EXDEV" in walker
    assert "owner_directory == nullptr && root[1]" in walker
    assert "owner_directory == nullptr && pathname[0] != '/'" in walker
    assert "set_dirfd_anchor(candidate, anchor.release(), true)" in walker
    for name in ("auto open_read_file_as_impl(", "auto vfs_stat_resolved_beneath_as(", "auto vfs_readlink_resolved_beneath_as("):
        body = body_after(source, name)
        assert "acquire_confined_owner_lookup(" in body
        assert "&lookup, directory" in " ".join(body.split())
        assert "REQUEST_CREDENTIALS" in body
    declarations = (ROOT / "modules/kern/src/vfs/vfs.hpp").read_text()
    assert declarations.count("File* directory = nullptr") >= 3


def test_remote_directory_paths_keep_exact_descriptor_and_snapshot():
    classifier = (ROOT / "modules/kern/src/net/wki/auth_policy.cpp").read_text()
    classifier = classifier[classifier.index("case OP_VFS_PATH_AS:"):classifier.index("case OP_VFS_STAT_AS:")]
    assert "VfsPathOperation::READLINK_AT" in classifier and "VfsPathOperation::STAT_AT" in classifier
    assert "WkiPolicy::VFS_READ" in classifier and "WkiPolicy::IDENTITY_SAME_DOMAIN" in classifier
    source = (ROOT / "modules/kern/src/net/wki/remote_vfs.cpp").read_text()
    sender = body_after(source, "auto remote_directory_path_request(")
    assert "auto const REQUEST_CREDENTIALS = credentials" in sender
    assert "MutexGuard io_guard(context->io_lock)" in sender
    assert "auto* state = context->proxy" in sender
    assert "&context->remote_fd" in sender
    assert "encode(REQUEST_CREDENTIALS)" in sender
    assert "acquire_vfs_proxy_lane" not in sender
    assert "reply.operation != operation" in sender
    assert sender.index("wki_vfs_path_response_valid") < sender.index("std::memcpy(output")
    owner = source[source.index("case OP_VFS_PATH_AS: {"):]
    owner = owner[:owner.index("bool const REALPATH = request.operation == VfsPathOperation::REALPATH;")]
    assert "map_vfs_read_identity(identity_policy, supplied, mapped)" in owner
    assert "find_remote_fd(channel_identity, relative.directory_fd)" in owner
    assert "vfs_retain_file(fd->file)" in owner
    assert owner.index("s_vfs_lock.unlock()") < owner.index("vfs_stat_resolved_beneath_as")
    assert "directory.get()" in owner and "vfs_readlink_resolved_beneath_as" in owner
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    for name in ("auto vfs_readlinkat(", "auto vfs_statat(", "auto vfs_faccessat("):
        body = body_after(core, name)
        assert "handle.remote_relative_path()" in body
        assert "handle.dirfd_anchor()" in body
        assert "handle.authorization().credentials" in body
    lookup = (ROOT / "modules/kern/src/vfs/lookup.cpp").read_text()
    assert "remote_relative_path_ = other.remote_relative_path_" in lookup
    assert "other.remote_relative_path_.fill" in lookup
    assert "remote_relative_path_.fill" in lookup


def test_remote_open_at_reserves_before_effect_and_never_uses_path_authority():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    caller = body_after(core, "static auto open_at_for_task(")
    assert caller.index("auto const REQUEST_CREDENTIALS") < caller.index("for (unsigned attempt")
    assert "false, &REQUEST_CREDENTIALS" in " ".join(caller.split())
    branch = caller[caller.index("handle.remote_relative_path() != nullptr"):caller.index("if ((args.flags & ker::vfs::O_NOFOLLOW)")]
    assert branch.index("descriptor.emplace(args.task)") < branch.index("wki_remote_vfs_open_at")
    assert branch.index("vfs_file_set_path") < branch.index("wki_remote_vfs_open_at") < branch.index("descriptor->install")
    assert "handle.authorization().credentials" in branch
    assert "apply_open_truncation" not in branch and "metadata_cache" not in branch
    assert "vfs_path_namespace_generation = 0" in branch
    for name in ("void cache_notify_attach_file(", "auto stream_cache_read_eligible("):
        assert "file->vfs_path_namespace_generation == 0" in body_after(core, name)
    source = (ROOT / "modules/kern/src/net/wki/remote_vfs.cpp").read_text()
    sender = body_after(source, "auto wki_remote_vfs_open_at(")
    assert "auto* state = original->proxy" in sender and "acquire_vfs_proxy_lane" not in sender
    assert sender.index("PinPreparation preparation(state, PinPurpose::ORDINARY_OPEN, prepared)") < sender.index("vfs_proxy_send_and_wait")
    assert sender.index("wki_vfs_path_open_result_valid") < sender.index("preparation.publish()")
    assert "io_guard" not in sender and "context.release()" not in sender
    preparation = body_after(source, "PinPreparation(ProxyVfsState* state, PinPurpose purpose, ker::vfs::File& prepared")
    assert preparation.index("acquire_vfs_proxy_open_ref") < preparation.index("link()") < preparation.index("new (std::nothrow) RemoteFileContext")
    owner_class = body_after(source, "class PinPreparation {")
    assert "candidate->detach_locked(true)" in owner_class
    assert "retired.finish_file = borrowed_file_ && abandoned" in owner_class
    assert "if (borrowed_file_ && !abandoned) retired.file = nullptr" in owner_class
    assert "ker::vfs::vfs_put_file(retired.file)" in owner_class
    assert "&original->remote_fd" in sender and "encode(REQUEST_CREDENTIALS)" in sender
    owner = source[source.index("case OP_VFS_PATH_AS: {"):]
    owner = owner[:owner.index("bool const REALPATH = request.operation == VfsPathOperation::REALPATH;")]
    assert "map_vfs_open_identity(identity_policy, supplied, relative.open_flags, mapped)" in owner
    assert owner.index("RemoteFdReservation reservation") < owner.index("vfs_open_file_resolved_beneath_as") < owner.index("reservation.publish")
    assert "g_remote_fds.extract(it)" in owner and "vfs_put_file(orphan)" in owner
    reservation = body_after(source, "class RemoteFdReservation")
    assert reservation.index("prepared.emplace") < reservation.index("s_vfs_lock.lock()")
    assert "!it->second.active && !it->second.retiring" in reservation
    mark = body_after(source, "void wki_remote_vfs_mark_server_fds_for_channel(")
    assert "if (rfd.retiring ||" in mark and "if (!rfd.active ||" not in mark


def test_native_realpath_uses_retained_lookup_before_legacy_caches():
    source = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    public = body_after(source, "auto vfs_realpath(")
    assert public.index("REQUEST_CREDENTIALS =") < public.index("realpath_native_as(task, REQUEST_CREDENTIALS")
    assert public.index("if (NATIVE != RESOLVE_FAST_PATH_DECLINED) return NATIVE;") < public.index("realpath_resolve_visible_path(")
    native = body_after(source, "auto realpath_native_as(", )
    assert "acquire_local_component_lookup" in native
    assert "LookupIntent::STAT, LookupFollowPolicy::FOLLOW_FINAL" in native
    assert "&REQUEST_CREDENTIALS" in native and "task->credentials()" not in native
    assert "consume_lookup_locked(lookup, false, consume, &context, task, &REQUEST_CREDENTIALS)" in native
    assert "g_vfs_namespace_publication_mutex" in native
    assert "stat_owner_lookup(handle, &metadata)" in native
    assert "lookup_visible_path_locked(handle, args.root" in native
    assert "handle.authorization().root_hash" in body_after(source, "auto lookup_visible_path_locked(")
    assert native.index("context.length + 1 > capacity") < native.index("std::memcpy(output") < native.index("*length = context.length")
    assert "metadata_cache" not in native and "readlink_resolved" not in native
    assert "TYPE != FSType::PROCFS" in native
    proc = native[native.index("if (!args.remote && handle.mount()->fs_type == FSType::PROCFS)"):native.index("} else if (!args.remote)")]
    assert "static_cast<const File*>(handle.backend_parent())" in proc
    assert "handle.requires_directory() && !file->is_directory" in proc
    for forbidden in ("procfs_fill_stat", "procfs_lookup", "procfs_readlink", "current_procfs", "find_task_by_pid", "vfs_open"):
        assert forbidden not in proc
    owner = body_after(source, "auto stat_owner_lookup(")
    assert "FSType::PROCFS" not in owner



def test_remote_realpath_delegates_complete_remainder_and_stages_output():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    client = body_after(core, "auto realpath_native_as(")
    remote = client[client.index("if (TYPE == FSType::REMOTE)"):]
    assert "lookup.remote_relative_path() != nullptr" in remote
    assert "wki_remote_vfs_realpath_at(lookup.dirfd_anchor(), lookup.remote_relative_path()" in remote
    assert remote.index("wki_remote_vfs_realpath_path") < remote.index("MutexGuard publication_guard")
    assert "REQUEST_CREDENTIALS, lookup.requires_directory()" in " ".join(remote.split())
    assert remote.index("lookup_request_context_matches") < remote.index("copy_mount_path_exact") < remote.index("consume(lookup, &context)")
    assert "if (REMOTE < 0) return static_cast<int>(REMOTE)" in remote
    owner = body_after(core, "auto vfs_realpath_resolved_beneath_as(")
    assert owner.index("REQUEST_CREDENTIALS = credentials") < owner.index("for (unsigned attempt") < owner.index("acquire_confined_owner_lookup")
    assert owner.index("lookup_is_beneath_export") < owner.index("consume_lookup_locked") < owner.index("std::memcpy(output")
    for forbidden in ("get_current_task", "task.credentials()", "readlink_resolved", "cache_lookup"):
        assert forbidden not in owner
    source = (ROOT / "modules/kern/src/net/wki/remote_vfs.cpp").read_text()
    retained = body_after(source, "auto remote_directory_path_request(")
    assert "VfsPathOperation::REALPATH_AT" in retained and "WKI_VFS_FEATURE_REALPATH_AT" in retained
    assert retained.index("WkiChannelIdentity const IDENTITY") < retained.index("invoke(OP_VFS_FEATURES") < retained.index("invoke(OP_VFS_PATH_AS")
    assert "&IDENTITY" in retained
    assert retained.index("wki_vfs_realpath_result_valid") < retained.index("return -ERANGE") < retained.index("std::memcpy(output")
    assert "vfs_proxy_read_with_retry" not in retained
    sender = body_after(source, "auto wki_remote_vfs_realpath_path(")
    for required in ("REQUEST_CREDENTIALS = credentials", "WKI_CAP_VFS_PATH_IDENTITY", "WKI_CAP_VFS_STAT_IDENTITY", "mapped_wire_path_status", "ProxyLifecycleRefGuard"):
        assert required in sender
    assert sender.index("wki_vfs_path_response_valid") < sender.index("wki_vfs_realpath_result_valid") < sender.index("std::memcpy(buf")
    assert "return -ERANGE" in sender and "std::min(bufsize" not in sender
    handler = body_after(source, "case OP_VFS_PATH_AS: {")
    assert "g_wki.vfs_realpath_enabled" in handler
    assert handler.index("map_vfs_read_identity") < handler.index("vfs_realpath_resolved_beneath_as")
    classifier = (ROOT / "modules/kern/src/net/wki/auth_policy.cpp").read_text()
    assert "header.operation != VfsPathOperation::REALPATH" in classifier



def test_replaced_exec_worker_cannot_publish_credentials():
    resource = (ROOT / "modules/kern/src/platform/sched/resource_process.cpp").read_text()
    mutation = resource.split("auto change_credentials(", 1)[1].split("auto initialize_credentials(", 1)[0]
    assert mutation.index("request_replaced_exec_thread_retirement(subject)") < mutation.index("population_lock.lock_irqsave()")
    assert "return -ECANCELED;" in mutation
    exit_source = (ROOT / "modules/kern/src/syscalls_impl/process/exit.cpp").read_text()
    retirement = exit_source.split("auto request_replaced_exec_thread_retirement(", 1)[1].split("void exit_current_if_process_exit_requested()", 1)[0]
    assert "if (!subject.is_thread) return false;" in retirement
    assert "subject.pagemap == owner->pagemap" in retirement
    assert "store_process_exit_request(&subject, 0, 0)" in retirement
    assert "process_exit_requested.load(std::memory_order_acquire)" in retirement
    assert "wos_thread_exit_current" not in retirement and "kern_yield" not in retirement
    process = (ROOT / "modules/kern/src/syscalls_impl/process/process.cpp").read_text()
    # Each currently exposed credential mutation has its own publication scope.
    # There must be no second mutation in that scope or an unguarded call.
    for segment in process.split("change_credentials(")[:-1]:
        guard = segment.rfind("SharedVmemPublicationGuard publication_guard;")
        assert guard >= 0 and "}" not in segment[guard:]
    assert process.count("change_credentials(") == 6


def test_file_mapping_uses_one_description_and_preflights_policy():
    vmem = (ROOT / "modules/kern/src/syscalls_impl/vmem/sys_vmem.cpp").read_text()
    mapping = body_after(vmem, "auto file_allocate(")
    assert mapping.count("vfs_get_file_retain(task, fd)") == 1
    assert "vfs_fstat(fd," not in mapping and "eager_file" not in mapping
    assert mapping.index("retained_file(") < mapping.index("vfs_fstat_file(file, &st)") < mapping.index("authorize_file_mapping(")
    assert mapping.index("POLICY_RET != 0") < mapping.index("SharedVmemPublicationGuard publication_guard") < mapping.index("release_fixed_mmap_range_locked(")
    assert "file_mmap_cached_page_for_file(file, st," in mapping
    assert "read_file_mapping_exact(file," in mapping
    assert mapping.count("register_file_mmap_range(task->pagemap, vaddr, REQUESTED_SIZE, offset, file, WRITEBACK)") == 3
    assert "register_file_mmap_range_from_fd" not in vmem
    protect = vmem[vmem.index("case ker::abi::vmem::ops::PROTECT:"):vmem.index("case ker::abi::vmem::ops::MREMAP:")]
    assert protect.index("SharedVmemPublicationGuard") < protect.index("authorize_shared_file_protection_locked(") < protect.index("protect_shared_vmem_range_locked(") < protect.index("unify_page_flags(")
    preflight = body_after(vmem, "auto authorize_shared_file_protection_locked(")
    assert "authorize_file_mapping(" in preflight and "file_mount_read_only(*file)" in preflight
    assert "vfs_get_file" not in preflight and "vfs_put_file" not in preflight
    registration = body_after(vmem, "auto register_file_mmap_range(")
    assert registration.index("g_file_mmap_ranges_lock.lock()") < registration.index("push_back(RANGE)") < registration.index("if (OK) ker::vfs::vfs_retain_file(file)") < registration.index("g_file_mmap_ranges_lock.unlock()")
    assert "!range.writeback" in body_after(vmem, "auto collect_file_mmap_sync_ranges(")
    assert "selftest_file_mapping_admission" in (ROOT / "modules/kern/src/test/mm_ktest.cpp").read_text()


def test_sync_preflights_all_ranges_and_serializes_mapping_identity():
    vmem = (ROOT / "modules/kern/src/syscalls_impl/vmem/sys_vmem.cpp").read_text()
    sync = body_after(vmem, "auto sync_file_mmap_range_impl(")
    assert sync.index("collect_file_mmap_sync_ranges(") < sync.index("preflight_file_mmap_sync_ranges(") < sync.index("for (size_t range_index") < sync.index("write_file_mapping_bytes(")
    preflight = body_after(vmem, "auto preflight_file_mmap_sync_ranges(")
    assert preflight.index("FILE_DELTA > UINT64_MAX - range.file_offset") < preflight.index("authorize_file_mapping_writeback(") < preflight.index("snapshot_file_mmap_page(") < preflight.index("range.skip_writeback = true")
    assert "range.sync_end - range.sync_start" in preflight and "return ADMISSION_RET" in preflight
    assert "write_file_mapping_bytes(" not in preflight
    assert sync.index("if (range.skip_writeback)") < sync.index("snapshot_file_mmap_page(")
    msync = body_after(vmem, "auto mmap_msync(")
    assert msync.index("SharedVmemPublicationGuard publication_guard") < msync.index("sync_file_mmap_range(*task,")
    teardown = body_after(vmem, "void release_file_mmap_ranges_for_pagemap(")
    assert teardown.count("sync_file_mmap_range_quiesced(") == 1
    assert teardown.index("sync_file_mmap_range_quiesced(pagemap, 0, USER_SPACE_END + 1)") < teardown.index("for (;;)") < teardown.index("unregister_file_mmap_range(")
    assert "selftest_file_mmap_sync_preflight" in (ROOT / "modules/kern/src/test/mm_ktest.cpp").read_text()


def test_writeback_and_replay_use_retained_file_policy():
    vmem = (ROOT / "modules/kern/src/syscalls_impl/vmem/sys_vmem.cpp").read_text()
    writeback = body_after(vmem, "auto write_file_mapping_bytes(")
    admission = body_after(vmem, "auto authorize_file_mapping_writeback(")
    assert admission.index("count - 1 > MAX_OFFSET - offset") < admission.index("authorize_file_description_access") < admission.index("file_mount_read_only(*file)")
    assert writeback.index("authorize_file_mapping_writeback(file, count, offset)") < writeback.index("while (written < count)") < writeback.index("vfs_pwrite_file(")
    assert "file->fops->vfs_write(file," not in writeback and "vfs_put_file(" not in writeback
    assert "vfs_cache_notify_file_data_changed(" not in writeback
    assert "return static_cast<int>(RET)" in writeback and "return -EIO" in writeback
    compute = (ROOT / "modules/kern/src/net/wki/remote_compute.cpp").read_text()
    replay = body_after(compute, "void write_proxy_output(")
    assert replay.index("vfs_get_file_retain(proxy, 1)") < replay.index("vfs_pwrite_file(file, output_data, output_len, 0)") < replay.index("vfs_put_file(file)")
    assert "fd_table.lookup" not in replay and "file->fops->vfs_write" not in replay


def test_retained_file_policy_denies_before_data_and_metadata_effects():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    direct = body_after(core, "auto vfs_write_file_direct(")
    assert direct.index("vfs_file_can_write(") < direct.index("file_mount_read_only(*f)") < direct.index("tmpfs_write_append(")
    assert direct.index("file_mount_read_only(*f)") < direct.index("f->fops->vfs_write(") < direct.index("cache_notify_file_data_changed_impl(")
    bounced = body_after(core, "auto vfs_write_user_bounced(")
    assert bounced.index("file_mount_read_only(*file)") < bounced.index("copy_from_task_partial(")
    positional = body_after(core, "auto vfs_pwrite_file(")
    assert positional.index("validate_positional_offset(") < positional.index("file_mount_read_only(*f)") < positional.index("vfs_pwrite_user_bounced(")
    assert positional.index("file_mount_read_only(*f)") < positional.index("f->fops->vfs_write(")
    remote = (ROOT / "modules/kern/src/net/wki/remote_vfs.cpp").read_text()
    metadata = body_after(remote, "auto remote_file_metadata_request(")
    assert metadata.index("TimestampUpdate::prepare(") < metadata.index("file_mount_read_only(*file)") < metadata.index("MutexGuard io_guard(")
    xattr = body_after(core, "auto dispatch_xattr_file(")
    assert xattr.index("file_mount_read_only(*file)") < xattr.index("publication_guard.emplace(")


def test_exec_nosuid_uses_retained_mount_policy():
    mount = (ROOT / "modules/kern/src/vfs/mount.cpp").read_text()
    assert "mount->nosuid = (flags & MOUNT_FLAG_NOSUID) != 0;" in mount
    assert "out->nosuid = mp->nosuid;" in mount
    header = (ROOT / "modules/kern/src/vfs/mount.hpp").read_text()
    policy = body_after(header, "inline auto file_mount_policy(")
    assert "file.mount_owner == nullptr" in policy and "file.mount_policy.restricted_by" in policy
    assert ".read_only = file.mount_owner->read_only" in policy and ".nosuid = file.mount_owner->nosuid" in policy
    assert "return file_mount_policy(file).nosuid" in body_after(header, "inline auto file_mount_nosuid(")
    image = (ROOT / "modules/kern/src/syscalls_impl/process/exec.cpp").read_text()
    assert image.count("bool const EXEC_NOSUID = vfs::file_mount_nosuid(*exec_file);") == 2
    # Spawn transition, native stack credentials, and retained native image.
    assert image.count(".nosuid = EXEC_NOSUID") == 3
    assert image.count(".nosuid = image.nosuid") == 4
    quiescence = image.split("auto prepare_native_exec_quiescence(", 1)[1].split("auto commit_native_exec_image(", 1)[0]
    assert ".nosuid = image.nosuid" in quiescence
    assert "&image.admitted_credentials" in quiescence
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    stats = body_after(core, "static auto statvfs_for_file(")
    assert stats.index("mount = file->mount_owner") < stats.index("find_mount_point(")
    assert "mount == nullptr && file->vfs_path != nullptr" in stats
    assert "statvfs_for_mount(*mount, buf)" in stats
    assert "if (result == 0 && mount.nosuid) buf->f_flag |= ST_NOSUID;" in core
    remote_publish = core.split("struct RemoteMountPublishContext", 1)[1].split("// Check for PARTUUID= prefix", 1)[0]
    assert "publish_context{.lookup = &lookup, .task = authorization_task, .credentials = &CREDENTIALS, .flags = flags}" in remote_publish
    assert '"remote", nullptr, context.flags, nullptr, private_data, fops, &prepared' in ' '.join(remote_publish.split())
    proc = (ROOT / "modules/kern/src/vfs/fs/procfs.cpp").read_text()
    assert 'if (mount_snapshot.nosuid || (INSIDE_ROOT && root.nosuid())) append(",nosuid");' in proc

def test_rename_pair_keeps_explicit_admitted_identity():
    source = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    public = body_after(source, "auto vfs_renameat(")
    assert public.index("REQUEST_CREDENTIALS = task->credentials()") < public.index("for (unsigned attempt")
    assert public.count("&REQUEST_CREDENTIALS") == 3
    assert "consume_lookup_pair_locked" in public and "vfs_consume_lookup_pair(" not in public
    pair = body_after(source, "auto consume_lookup_pair_locked(")
    for token in ("first.authorization().credentials != second.authorization().credentials", "lookup_request_context_matches",
                  "request_task", "request_credentials", "CURRENT_NAMESPACE_GENERATION", "mount_still_matches", "FIRST_VALID", "SECOND_VALID"):
        assert token in pair, token
    assert pair.index("authorized(first)") < pair.index("consumer(first, second, context)")
    owner = body_after(source, "auto vfs_rename_resolved_beneath_as(")
    assert owner.count("acquire_confined_owner_lookup") == 2
    assert owner.count("lookup_is_beneath_export") == 2
    assert "consume_lookup_pair_locked" in owner and "current_metadata_credentials" not in owner
    assert "task.credentials()" not in owner


def test_linkat_pair_keeps_one_snapshot_through_both_lookups_and_retries():
    source = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    public = body_after(source, "auto vfs_linkat(")
    assert public.count("task->credentials()") == 1
    assert public.index("REQUEST_CREDENTIALS = task->credentials()") < public.index("for (unsigned attempt")
    assert public.count("&REQUEST_CREDENTIALS") == 3
    assert "consume_lookup_pair_locked(source, destination, true, consume_link, nullptr, task, &REQUEST_CREDENTIALS)" in public
    assert "vfs_consume_lookup_pair(" not in public
    assert "source.mount_policy().read_only || destination.mount_policy().read_only" in public
    assert public.index("g_vfs_namespace_publication_mutex") < public.index("consume_lookup_pair_locked")


def test_ptrace_exception_stop_publishes_before_nonreturning_handoff():
    source = (ROOT / "modules/kern/src/platform/debug/ptrace.cpp").read_text()
    stop = source[source.index("auto report_user_stop_common("):source.index("auto report_user_stop(")]
    assert "frame.flags &= ~X86_RFLAGS_TF" in stop
    assert "[](Task& stopped) { static_cast<void>(publish_stop_event(stopped)); }" in stop
    scheduler = (ROOT / "modules/kern/src/platform/sched/scheduler.cpp").read_text()
    wait = scheduler[scheduler.index("void place_task_in_wait_queue("):scheduler.index("inline auto queue_reschedule_request(")]
    assert wait.index("current_task->context.regs = gpr") < wait.index("transition_make_waiting_locked") < wait.index("on_parked(*current_task)")
    assert wait.index("on_parked(*current_task)") < wait.index("sys::context_switch::switch_to")
    assert "wos_start_kernel_thread(next_task->context.frame.rsp, next_task->kthread_entry)" in wait
    assert "wos_deferred_task_switch_return(&gpr, &frame);" in wait


def test_lookup_consumers_use_selected_mount_policy():
    source = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    assert not re.search(r"\b(?:handle|lookup|source|destination)\.mount\(\)->(?:read_only|nosuid)", source)
    owner_open = body_after(source, "auto open_read_file_as_impl(")
    assert owner_open.index("handle.mount_policy().read_only") < owner_open.index("MountOpenFileReservation")
    lookup = (ROOT / "modules/kern/src/vfs/lookup.cpp").read_text()
    select = body_after(lookup, "void LookupHandleBuilder::set_mount(")
    assert select.index("handle.mount_ref_ = std::move(mount)") < select.index("handle.mount_policy_ =")
    assert "selected->read_only" in select and "selected->nosuid" in select
    assert "dirfd_anchor" not in select
    recover = body_after(source, "auto copy_directory_lookup_path(")
    assert recover.index("backend_path[1] == '\\0'") < recover.index("size_t const PREFIX")
    assert "return copy_path_string(mount_path.data(), out.data(), out.size())" in recover
    cases = (ROOT / "modules/kern/src/test/credential_ktest.cpp").read_text()
    crossing = body_after(cases, "KTEST(Credentials, NativeLookupPolicyFollowsSelectedMountAcrossDirectoryAnchors)")
    for token in ["vfs_fchdir", "vfs_pwrite_file", "vfs_mkdirat", "../rw/file", "absolute", "nested/file", "-EROFS"]:
        assert token in crossing


def test_open_and_exec_replacement_preserve_admitted_view_policy():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    reservation = core[core.index("class MountOpenFileReservation"):core.index("auto vfs_destroy_file(")]
    assert "MountOpenFileReservation(lookup.mount(), lookup.mount_policy())" in reservation
    assert "file->mount_policy.restricted_by(policy_)" in reservation
    assert "policy.restricted_by({.read_only = mount_->read_only, .nosuid = mount_->nosuid})" in reservation
    for signature in ("auto vfs_open_resolved_for_task(", "static auto vfs_open_file_impl("):
        body = body_after(core, signature)
        assert body.index("stable_lookup->mount() != mount") < body.index("OPEN_POLICY.read_only")
        assert body.index("OPEN_POLICY.read_only") < body.index("MountOpenFileReservation mount_file_reservation{mount, OPEN_POLICY}") < body.index("switch (mount->fs_type)")
    clone = body_after(core, "auto vfs_stabilize_exec_file(")
    policy = clone.index("view->mount_policy = view->mount_policy.restricted_by(file_mount_policy(*SOURCE))")
    assert clone.index("view.reset(candidate)") < policy < clone.index("SOURCE->mount_owner != nullptr")
    assert policy < clone.index("*owned = view.release()") < clone.index("vfs_put_file(SOURCE)")
    proc = body_after(core, "auto open_procfs_lookup_as(")
    assert "mount_reservation.emplace(handle)" in proc
    assert "file->mount_policy =" not in proc  # Shared FD reopen must stay untouched.
    for signature, spelling in [("auto prepare_remote_cwd(", "reservation(lookup)"),
                                ("auto open_remote_read_file_as(", "reservation{lookup}"),
                                ("auto open_read_file_as_impl(", "reservation{handle}"),
                                ("auto vfs_pin_directory_resolved_beneath_as(", "reservation(handle)")]:
        assert spelling in body_after(core, signature)
    exec_source = (ROOT / "modules/kern/src/syscalls_impl/process/exec.cpp").read_text()
    assert exec_source.count("bool const EXEC_NOSUID = vfs::file_mount_nosuid(*exec_file);") == 2
    tests = (ROOT / "modules/kern/src/test/credential_ktest.cpp").read_text()
    assert "ReplacementPreservesSourceViewPolicyAndIndependentLifetime" in tests


def test_xfs_lookup_mutations_consume_explicit_readonly_policy():
    backend = (ROOT / "modules/kern/src/vfs/fs/xfs/xfs_vfs.cpp").read_text()
    header = (ROOT / "modules/kern/src/vfs/fs/xfs/xfs_vfs.hpp").read_text()
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    names = ("chmod", "chown", "utimens", "setxattr", "removexattr", "mkdir", "symlink", "unlink", "rmdir", "link", "rename")
    for name in names:
        signature = "auto xfs_" + name + "_lookup("
        body = body_after(backend, signature)
        assert body.index("read_only || MOUNT->read_only") < body.index("XfsMetadataGuard")
        declaration = header[header.index(signature):].split(";", 1)[0]
        assert "bool read_only = false" in declaration
        if name in ("link", "rename"):
            assert body.index("MOUNT != destination.mount()") < body.index("read_only || MOUNT->read_only")
        # Every VFS call must carry the selected policy; default false is only
        # for direct backend callers with their existing native mount contract.
        for call in re.finditer(r"xfs_" + name + r"_lookup\(", core):
            cursor = call.end()
            depth = 1
            while depth:
                depth += (core[cursor] == "(") - (core[cursor] == ")")
                cursor += 1
            args = core[call.end():cursor - 1]
            if name in ("link", "rename"):
                assert "source.mount_policy().read_only || destination.mount_policy().read_only" in args
            else:
                assert "handle.mount_policy().read_only" in args
    assert "XfsLookupViewDenialsPreserveWritableBackendAndOutputs" in (ROOT / "modules/kern/src/test/credential_ktest.cpp").read_text()


def test_lookup_context_includes_nonreused_cwd_anchor_identity():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    lookup = (ROOT / "modules/kern/src/vfs/lookup.hpp").read_text()
    storage = (ROOT / "modules/kern/src/vfs/lookup.cpp").read_text()
    assert "uint64_t cwd_anchor_identity{}" in lookup
    allocator = body_after(core, "auto take_directory_anchor_identity(")
    assert "value == UINT64_MAX ? 0 : value + 1" in allocator
    assert "while (value != 0)" in allocator and "compare_exchange_weak" in allocator
    assert "DirectoryAnchor{}" not in core
    factory = body_after(core, "auto allocate_directory_anchor(")
    assert "IDENTITY != 0" in factory and "DirectoryAnchor(IDENTITY)" in factory
    validator = body_after(core, "auto lookup_request_context_matches(")
    assert validator.index("path_metadata_lock.lock_irqsave()") < validator.index("authorization.cwd_anchor_identity")
    assert validator.index("authorization.cwd_anchor_identity") < validator.index("path_metadata_lock.unlock_irqrestore")
    for signature in ("auto acquire_local_component_lookup(", "auto vfs_acquire_lookup("):
        body = body_after(core, signature)
        assert body.index("path_metadata_lock.lock_irqsave()") < body.index("authorization.cwd_anchor_identity =")
        assert body.index("authorization.cwd_anchor_identity =") < body.index("path_metadata_lock.unlock_irqrestore")
    publish = body_after(core, "auto change_directory_at(")
    assert publish.index("handle.authorization().cwd_anchor_identity") < publish.index("path_owner->cwd_anchor.swap")
    assert "other.authorization_ = {};" in storage
    assert "authorization_ = {};" in body_after(storage, "void LookupHandle::reset()")
    assert "TmpfsLookupRejectsSameTextCwdReplacement" in (ROOT / "modules/kern/src/test/vfs_ktest.cpp").read_text()
    assert "XfsLookupRejectsSameTextCwdReplacement" in (ROOT / "modules/kern/src/test/vfs_ktest.cpp").read_text()



def test_lookup_context_includes_canonical_root_anchor_identity():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    lookup = (ROOT / "modules/kern/src/vfs/lookup.hpp").read_text()
    assert "uint64_t root_anchor_identity{}" in lookup
    validator = body_after(core, "auto lookup_request_context_matches(")
    assert validator.index("path_metadata_lock.lock_irqsave()") < validator.index("authorization.root_anchor_identity") < validator.index("path_metadata_lock.unlock_irqrestore")
    for signature in ("auto acquire_local_component_lookup(", "auto vfs_acquire_lookup("):
        body = body_after(core, signature)
        assert body.index("path_metadata_lock.lock_irqsave()") < body.index("authorization.root_anchor_identity =") < body.index("path_metadata_lock.unlock_irqrestore")
        assert "directory_anchor_identity(path_owner->root_anchor)" in body
    publish = body_after(core, "auto change_directory_at(")
    assert publish.index("handle.authorization().root_anchor_identity") < publish.index("path_owner->cwd_anchor.swap")
    realpath = body_after(core, "auto realpath_native_as(")
    capture = body_after(core, "auto capture_lookup_root(")
    assert capture.index("path_metadata_lock.lock_irqsave()") < capture.index("candidate.anchor = owner->root_anchor") < capture.index("path_metadata_lock.unlock_irqrestore")
    assert "capture_lookup_root(*task, lookup, context.root)" in realpath
    visible = body_after(core, "auto lookup_visible_path_locked(")
    assert visible.index("handle.authorization().root_anchor_identity") < visible.index("const char* visible =")
    tests = (ROOT / "modules/kern/src/test/vfs_ktest.cpp").read_text()
    assert "TmpfsLookupRejectsSameTextRootReplacement" in tests and "XfsLookupRejectsSameTextRootReplacement" in tests
    fixture = body_after(core, "auto vfs_selftest_lookup_cwd_identity(")
    for token in ("vfs_prepare_filesystem_context(caller, root_view, prepared)", "owner.root_anchor.swap(replacement)",
                  "(!root_identity || directory_anchor_identity(owner.cwd_anchor) == CWD_IDENTITY)",
                  "single(owned) == -EAGAIN", "single(global) == -EAGAIN", "two(owned, global) == -EAGAIN",
                  "context.calls == 0", "std::memcmp(&context.output, &SENTINEL", "single(owned) == 0"):
        assert token in fixture



def test_absolute_task_lookup_uses_owned_root_and_preserves_search_semantics():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    walk = body_after(core, "auto acquire_local_component_lookup(")
    assert "references.root" in walk
    assert walk.index("path_metadata_lock.lock_irqsave()") < walk.index("root_anchor = path_owner->root_anchor") < walk.index("path_metadata_lock.unlock_irqrestore")
    assert walk.index("authorization.root_hash =") < walk.index("directory_anchor_path(*root_anchor.get(), root, nullptr)")
    for token in ("view.root() != view.directory()", "authorization.root_identity_view = true", "!authorization.root_identity_view",
                  "OWNED_ROOT != nullptr ? OWNED_ROOT", "nullptr, nullptr, RESET_ROOT, restart"):
        assert token in walk
    fallback = body_after(core, "auto vfs_acquire_lookup(")
    assert "!already_resolved && authorization.root_anchor_identity != 0" in fallback
    validate = body_after(core, "auto validate_lookup_directory_view_locked(")
    assert "handle.authorization().root_identity_view && view.root() != view.directory()" in validate
    assert "!handle.authorization().root_identity_view, membership" in validate
    fixture = body_after(core, "auto vfs_selftest_owned_root_absolute_walk(")
    for token in ("vfs_rename(ROOT_PATH, MOVED)", 'seed(FILE_PATH, "fake")', 'std::memcmp(bytes.data(), "real", 4)',
                  '"/../../cwd/file"', "open_local_serialized(request", "FD == -EROFS", "metadata.st_ino == root_stat.st_ino",
                  "CHILD == -EACCES", "owner.root == ROOT_TEXT", "MOUNT->open_files.load() == BUSY"):
        assert token in fixture
    tests = (ROOT / "modules/kern/src/test/vfs_ktest.cpp").read_text()
    assert "TmpfsAbsoluteLookupRetainsRenamedRoot" in tests and "XfsAbsoluteLookupRetainsRenamedRoot" in tests



def test_relative_owned_root_lookup_intersects_native_views_and_policy():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    walk = body_after(core, "auto acquire_local_component_lookup(")
    for token in ("if (!physical_path && initial_view == nullptr) root_anchor = path_owner->root_anchor", "bool const CWD_MATCH",
                  "previous = process_view.root()", "boundary = process_view.root()", "admit_directory_view(previous, boundary",
                  "references.view.policy().restricted_by(process_view.policy())", "references.view.swap(merged)",
                  "references.view.root(), anchor.get(), authorization.credentials", "File* const RESET_ROOT"):
        assert token in walk
    bind = body_after(core, "auto local_walk_bind(")
    assert bind.index("anchor->mount_owner != mount") < bind.index("static_cast<ker::vfs::tmpfs::TmpNode*>(anchor->private_data)")
    fixture = body_after(core, "auto vfs_selftest_owned_root_absolute_walk(")
    for token in ("if (relative_paths)", "descriptors.ids[3]", "base_pin.get() : cwd.get()", "POLICY.read_only || i == 2",
                  "POLICY.nosuid || i != 0", '"../../outside"', "i == 2 ? -EPERM : -ENOENT", "directory_view.get() == original_views[i]",
                  "consume_relative", "relative.stat.st_ino == expected.st_ino"):
        assert token in fixture
    tests = (ROOT / "modules/kern/src/test/vfs_ktest.cpp").read_text()
    assert "TmpfsRelativeLookupIntersectsOwnedRootAndDirectoryViews" in tests
    assert "XfsRelativeLookupIntersectsOwnedRootAndDirectoryViews" in tests


def test_retained_directory_boundary_uses_native_objects_and_stages_outputs():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    tmpfs = (ROOT / "modules/kern/src/vfs/fs/tmpfs.cpp").read_text()
    xfs = (ROOT / "modules/kern/src/vfs/fs/xfs/xfs_vfs.cpp").read_text()
    boundary = body_after(core, "auto directory_member_path_locked(")
    wrapper = body_after(core, "auto vfs_directory_path_beneath_directory(")
    assert "current->mount_owner != root->mount_owner" in boundary
    assert boundary.index("directory->private_data == nullptr") < boundary.index("!directory->is_directory") < boundary.index("directory->mount_owner == nullptr")
    for forbidden in ("vfs_path", "vfs_acquire_lookup", "vfs_open", "credentials()", "get_current_task"):
        assert forbidden not in boundary
    assert wrapper.index("NativeDirectoryMembershipReferences references") < wrapper.index("publication_guard") < wrapper.index("directory_member_path_locked(")
    assert "MutexGuard" not in boundary and "references.capture(child_mount.get(), MOUNTS, covered)" in boundary
    native_member = body_after(core, "auto native_directory_member_path(")
    assert "root->private_data" in native_member and "directory->private_data" in native_member
    assert "tmpfs::tmpfs_directory_path_beneath" in native_member and "xfs::xfs_directory_path_beneath" in native_member
    assert boundary.index("native_directory_member_path(root, current") < boundary.index("LENGTH + 1 > size") < boundary.index("std::memcpy(buffer")
    assert boundary.index("std::memcpy(buffer") < boundary.index("*length = LENGTH")
    native = body_after(tmpfs, "auto directory_path(")
    assert "bounded && (root->unlinked || directory->unlinked)" in native
    assert "bounded && (node->parent == nullptr || node->parent == node)" in native
    assert "directory_path(root, directory, out, capacity, true, credentials)" in body_after(tmpfs, "auto tmpfs_directory_path_beneath(")
    native = body_after(xfs, "auto directory_path_at_inode(")
    assert "retained != boundary" in native and "retained->nlink == 0" in native
    assert "boundary != nullptr ? boundary->ino : ctx->root_ino" in native
    assert 'boundary != nullptr && current->ino == ctx->root_ino' in native
    assert "xfs_dir_entry_is_indexed(scan.parent, entry)" in native
    assert "RetainedDirectoryBoundarySurvivesRenameAndRejectsOutsideObjects" in (ROOT / "modules/kern/src/test/credential_ktest.cpp").read_text()


def test_owned_directory_views_narrow_policy_and_publish_outside_locks():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    header = (ROOT / "modules/kern/src/vfs/directory_view.hpp").read_text()
    tmpfs = (ROOT / "modules/kern/src/vfs/fs/tmpfs.cpp").read_text()
    xfs = (ROOT / "modules/kern/src/vfs/fs/xfs/xfs_vfs.cpp").read_text()
    view = core[core.index("struct DirectoryView {"):core.index("DirectoryViewRef::DirectoryViewRef(")]
    assert "File* const root" in view and "File* const directory" in view and "const MountPolicy inherited_policy" in view
    assert "DirectoryViewRef" not in view
    assert "vfs_retain_file(root)" in view and "vfs_retain_file(directory)" in view
    assert "vfs_put_file(root)" in view and "vfs_put_file(directory)" in view
    assert "Native mount flags remain per-lookup" in header
    prepare = body_after(core, "auto prepare_directory_view(")
    assert "REQUEST_CREDENTIALS = credentials" in prepare
    assert "previous.policy().restricted_by(additional_policy)" in prepare
    assert "mount_policy" not in prepare and "get_current_task" not in prepare
    assert prepare.index("DirectoryViewRef candidate(") < prepare.index("admit_directory_view(") < prepare.index("output.swap(candidate)")
    assert "publication_guard" not in prepare
    pins = body_after(core, "auto validate_directory_view_pin(")
    assert "FILE_ACCESS_SEARCH_ONLY" in pins and "refcount.load() <= 0" in pins
    admit = body_after(core, "auto admit_directory_view(")
    assert admit.index("publication_guard") < admit.index("directory_view_member_path(previous_root, root, nullptr, references, relative)") < admit.index("directory_view_member_path(root, directory, require_directory_search ? &credentials : nullptr, references, relative)")
    assert "mount_table_generation_snapshot()" in admit and "retiring.load" in admit
    for text, signature, lock in ((tmpfs, "auto directory_path(", "MutexGuard guard(tmpfs_lock)"),
                                  (xfs, "auto directory_path_at_inode(", "XfsMetadataGuard metadata_guard")):
        body = body_after(text, signature)
        assert body.index(lock) < body.index("authorize_file_access(*credentials") < body.index("std::memcpy(out,")
    assert "TmpfsOwnedDirectoryViewNarrowsAndRetiresOutsideLocks" in (ROOT / "modules/kern/src/test/credential_ktest.cpp").read_text()
    assert "XfsOwnedDirectoryViewNarrowsAndRetiresOutsideLocks" in (ROOT / "modules/kern/src/test/credential_ktest.cpp").read_text()


def test_serialized_lookup_releases_captured_references_after_publication():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    fallback = body_after(core[core.index("// Only native TMPFS/XFS steps run"):], "auto open_local_serialized(")
    assert fallback.index("LocalWalkReferences references;") < fallback.index("MutexGuard publication_guard")
    assert fallback.index("LookupHandle lookup{};") < fallback.index("MutexGuard publication_guard")
    assert "nullptr, nullptr, &references" in fallback
    assert fallback.index("MutexGuard publication_guard") < fallback.index("acquire_local_component_lookup(") < fallback.index("consume_lookup_locked(")
    walk = body_after(core, "auto acquire_local_component_lookup(")
    assert "serialized != (serialized_references != nullptr)" in walk
    assert "references.cwd || references.directory || references.view || references.procfs_view_origin" in walk
    assert "(references.membership && initial_view == nullptr)" in walk
    assert "auto& cwd_anchor = references.cwd" in walk and "auto& anchor = references.directory" in walk
    assert "std::unique_ptr<File, LocalWalkFileRelease> anchor" not in walk
    assert "DirectoryAnchorRef cwd_anchor" not in walk
    assert "serialized && path_owner->cwd_anchor && path_owner->cwd_anchor.get()->remote != nullptr" in walk
    fixture = body_after(core, "auto vfs_selftest_serialized_lookup_retirement(")
    assert "state.replacement->refcount.load() == 1" in fixture and "state.closes == 1" in fixture
    assert "-EIO, -ENOENT, -EACCES, -ENOTDIR, -EAGAIN" in fixture
    tests = (ROOT / "modules/kern/src/test/credential_ktest.cpp").read_text()
    assert "TmpfsSerializedLookupRetiresDescriptorsOutsideLocks" in tests
    assert "XfsSerializedLookupRetiresDescriptorsOutsideLocks" in tests


def test_directory_view_lookup_retains_boundary_and_checks_each_consumer():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    header = (ROOT / "modules/kern/src/vfs/lookup.hpp").read_text()
    impl = (ROOT / "modules/kern/src/vfs/lookup.cpp").read_text()
    assert "DirectoryViewRef directory_view_{}" in header
    assert "mount_policy_.restricted_by(directory_view_.policy())" in header
    assert "directory_view_ = std::move(other.directory_view_)" in impl
    assert "directory_view_ = {};" in body_after(impl, "void LookupHandle::reset()")
    acquire = body_after(core, "auto vfs_acquire_directory_view_lookup(")
    assert "REQUEST_CREDENTIALS = credentials" in acquire and "auto retained_view = view" in acquire
    assert acquire.index("LookupHandle candidate;") < acquire.index("MutexGuard publication_guard")
    assert acquire.index("admit_directory_view(") < acquire.index("copy_directory_lookup_path(") < acquire.index("acquire_local_component_lookup(")
    assert "&references, mount, &retained_view" in " ".join(acquire.split())
    assert acquire.index("acquire_local_component_lookup(") < acquire.index("output = std::move(candidate)")
    assert "out->reset" not in acquire and "vfs_acquire_lookup(" not in acquire
    bind = body_after(core, "auto local_walk_bind(")
    assert bind.index("epoch.confined_mount != nullptr && (mount == nullptr") < bind.index("if (mount == nullptr)")
    validate = body_after(core[core.index("auto admit_directory_view("):], "auto validate_lookup_directory_view_locked(")
    assert "mount == view.root()->mount_owner" in validate
    assert "view.root(), view.directory(), handle.authorization().credentials, true" in validate
    single = body_after(core[core.index("auto vfs_acquire_lookup("):], "auto consume_lookup_locked(")
    assert single.index("validate_lookup_directory_view_locked(handle)") < single.index("consumer(handle, context)")
    pair = body_after(core, "auto consume_lookup_pair_locked(")
    assert pair.index("validate_lookup_directory_view_locked(first)") < pair.index("validate_lookup_directory_view_locked(second)") < pair.index("consumer(first, second, context)")
    tests = (ROOT / "modules/kern/src/test/credential_ktest.cpp").read_text()
    assert "TmpfsLookupRetainsDirectoryViewBoundaryAndPolicy" in tests and "XfsLookupRetainsDirectoryViewBoundaryAndPolicy" in tests


def test_tmpfs_directory_authority_changes_invalidate_prior_component_search():
    tmpfs = (ROOT / "modules/kern/src/vfs/fs/tmpfs.cpp").read_text()
    commit = body_after(tmpfs, "void commit_metadata_locked(")
    assert "node.type == TmpNodeType::DIRECTORY" in commit
    assert "node.uid != prepared.uid" in commit and "node.gid != prepared.gid" in commit
    assert "node.mode != (prepared.mode & 07777U)" in commit
    assert "if (SEARCH_AUTHORITY_CHANGED) tmpfs_search_authority_generation_value.fetch_add" in commit
    assert commit.index("SEARCH_AUTHORITY_CHANGED =") < commit.index("node.uid = prepared.uid") < commit.index("tmpfs_search_authority_generation_value.fetch_add") < commit.index("tmpfs_fill_metadata_locked")
    for signature in ("auto chmod_locked(", "auto chown_locked("):
        assert "commit_metadata_locked(node, prepared, out)" in body_after(tmpfs, signature)
    validate = body_after(tmpfs, "auto tmpfs_validate_lookup_locked(")
    assert "tmpfs_namespace_generation_locked() != handle.namespace_generation_" in validate
    assert "tmpfs_search_authority_generation" not in validate
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    assert core.count(".tmpfs_search_generation = tmpfs::tmpfs_search_authority_generation()") == 2
    assert "lookup_search_authority_current(handle)" in body_after(core, "auto lookup_request_context_matches(")
    assert "lookup_search_authority_current(handle)" in body_after(core, "auto lookup_authorization_still_current(")
    assert "GENERATION == 0 || GENERATION == tmpfs::tmpfs_search_authority_generation()" in body_after(core, "auto lookup_search_authority_current(")
    fixture = body_after((ROOT / "modules/kern/src/vfs/core.cpp").read_text(), "auto vfs_selftest_directory_view_lookup(")
    assert '"search/leaf"' in fixture and "consume(held, user) == -EAGAIN" in fixture
    assert "context.calls == BEFORE_REVOCATION" in fixture


def test_xfs_directory_authority_invalidates_whole_path_across_mounts():
    xfs = (ROOT / "modules/kern/src/vfs/fs/xfs/xfs_vfs.cpp").read_text()
    commit = body_after(xfs, "auto commit_file_identity_locked(")
    assert "xfs_inode_isdir(ip)" in commit
    for field in ("mode", "uid", "gid"):
        assert f"ip->{field} != prepared.{field}" in commit
    bump = "if (RET == 0 && SEARCH_AUTHORITY_CHANGED) xfs_search_authority_generation_value.fetch_add"
    assert bump in commit
    assert commit.index("xfs_trans_capture_inode") < commit.index("SEARCH_AUTHORITY_CHANGED =") < commit.index("ip->mode =")
    assert commit.index("xfs_trans_commit(tp)") < commit.index(bump) < commit.index("fill_stat(ip, statbuf)")
    for name in ("chmod_inode_locked", "chown_inode_locked"):
        assert "commit_file_identity_locked(ip, prepared, statbuf)" in body_after(xfs, "auto " + name + "(")
    for name, helper in (("xfs_fchmod", "chmod_inode_locked"), ("xfs_fchown", "chown_inode_locked")):
        body = body_after(xfs, "auto " + name + "(")
        assert body.index("io_guard") < body.index("metadata_guard") < body.index(helper + "(")
    assert "std::atomic<uint64_t> xfs_search_authority_generation_value{1}" in xfs
    assert "std::memory_order_acquire" in body_after(xfs, "auto xfs_search_authority_generation(")
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    assert core.count(".xfs_search_generation = xfs::xfs_search_authority_generation()") == 2
    validation = body_after(core, "auto lookup_search_authority_current(")
    assert "handle.authorization().xfs_search_generation" in validation
    assert "XFS_GENERATION == 0 || XFS_GENERATION == xfs::xfs_search_authority_generation()" in validation
    prepared = body_after(core, "auto vfs_prepare_filesystem_context(")
    assert prepared.index("auto const XFS_SEARCH") < prepared.index("admit_directory_view(")
    assert prepared.index("XFS_SEARCH != xfs::xfs_search_authority_generation()") < prepared.index("output = std::move(candidate)")
    fixture = body_after(core, "auto vfs_selftest_xfs_search_authority(")
    assert '"/tmp/ktest_xfs_search/disk/guard/../../target"' in fixture
    assert "searched.mount()->fs_type == FSType::TMPFS" in fixture
    assert "phase < 3" in fixture and "for (bool explicit_request : {false, true})" in fixture
    assert "consume_pair(searched, direct, explicit_request)" in fixture
    assert "consume_pair(direct, searched, explicit_request)" in fixture
    assert "context.calls == 0" in fixture and "context.output.st_ino == 0x55AA" in fixture
    assert "acquire(VIA, user, fresh) == -EACCES" in fixture and "acquire(VIA, ROOT, fresh) == 0" in fixture
    assert "xfs::xfs_search_authority_generation() == SEARCH + 1" in fixture
    assert "tmpfs::tmpfs_search_authority_generation() == TMPFS_SEARCH" in fixture


def test_directory_view_rebase_owns_detached_pin_and_readmits_after_unlock():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    rebase = body_after(core, "auto vfs_rebase_directory_view(")
    assert 'if (!lookup.directory_view()) return -EINVAL' in rebase
    guard = rebase.index("ker::mod::sys::MutexGuard publication_guard")
    assert rebase.index("auto const REQUEST_CREDENTIALS = credentials") < guard
    assert rebase.index("auto source = lookup.directory_view()") < guard
    assert rebase.index("std::unique_ptr<File, LocalWalkFileRelease> pin") < guard
    producer = body_after(core, "auto pin_directory_view_lookup(")
    assert "MountOpenFileReservation reservation(mount)" in producer
    assert "MountOpenFileReservation reservation(handle)" not in producer
    assert producer.index("pin.reset(file)") < producer.index("reservation.attach(file)")
    assert "consume_lookup_locked(lookup, false, consume, &context, &task, &REQUEST_CREDENTIALS)" in rebase
    assert "if (RESULT != 0) return RESULT;\n    }" in rebase
    assert guard < rebase.index("if (directory_view_rebase_test_hook") < rebase.index("return prepare_directory_view(")
    assert "prepare_directory_view(source, REQUEST_CREDENTIALS, source.root(), pin.get(), {}, output, context.require_target_search)" in rebase
    for backend in ("tmpfs", "xfs"):
        assert f"{backend}::{backend}_pin_directory_lookup(caller," in producer
    view = core[core.index("struct DirectoryView {"):core.index("DirectoryViewRef::DirectoryViewRef(")]
    assert "DirectoryViewRef" not in view
    fixture = body_after(core, "auto vfs_selftest_directory_view_rebase(")
    assert "step < 16" in fixture and "BUSY + 1" in fixture
    assert "vfs_rename(CHILD, OUTSIDE)" in fixture and "vfs_chmod(CHILD, 0000)" in fixture
    assert "state.closes == CLOSES + 1" in fixture and "state.closes == state.pins + 2" in fixture
    assert "state.pins == BEFORE_STALE" in fixture and "output.get() == OLD" in fixture


def test_directory_identity_admission_does_not_bypass_current_search_or_grant_data():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    header = (ROOT / "modules/kern/src/vfs/lookup.hpp").read_text()
    assert "DirectoryViewAdmission admission = DirectoryViewAdmission::SEARCH" in header
    rebase = body_after(core, "auto vfs_rebase_directory_view(")
    assert "admission != DirectoryViewAdmission::SEARCH && admission != DirectoryViewAdmission::IDENTITY_ONLY" in rebase
    assert "context{&pin, admission == DirectoryViewAdmission::SEARCH}" in rebase
    assert "pin_directory_view_lookup(handle, context.require_target_search, pin)" in rebase
    assert "output, context.require_target_search)" in rebase
    prepare = body_after(core, "auto prepare_directory_view(")
    assert "REQUEST_CREDENTIALS, namespace_already_locked, require_directory_search" in re.sub(r"\s+", " ", prepare)
    for signature in ("auto vfs_prepare_directory_view(", "auto vfs_narrow_directory_view("):
        assert "require_directory_search" not in body_after(core, signature)
    acquire = body_after(core, "auto vfs_acquire_directory_view_lookup(")
    assert "retained_view.directory(), REQUEST_CREDENTIALS, true, true," in acquire
    validate = body_after(core[core.index("auto admit_directory_view("):], "auto validate_lookup_directory_view_locked(")
    assert "handle.authorization().credentials, true, !handle.authorization().root_identity_view, membership)" in re.sub(r"\s+", " ", validate)
    for name, backend in (("tmpfs.cpp", "tmpfs"), ("xfs/xfs_vfs.cpp", "xfs")):
        text = (ROOT / "modules/kern/src/vfs/fs" / name).read_text()
        pin = body_after(text, "auto " + backend + "_pin_directory_lookup(")
        assert pin.index("if (!handle.root_target())") < pin.index("if (require_target_search)")
        assert "FILE_ACCESS_SEARCH_ONLY" in pin and "O_DIRECTORY" in pin
    fixture = body_after(core, "auto vfs_selftest_directory_view_admission(")
    for token in ("vfs_chmod(TARGET, 0444)", "vfs_read_dir_entries(FD,", "READ > 0", "DirectoryViewAdmission::IDENTITY_ONLY",
                  "stale == -EAGAIN && calls == 0", "PIN_READ == static_cast<ssize_t>(-EBADF)",
                  "output.get() == source.get()", "state.closes == CLOSES + 1"):
        assert token in fixture


def test_directory_files_and_cwd_publish_and_retain_complete_owned_views():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    file_header = (ROOT / "modules/kern/src/vfs/file.hpp").read_text()
    assert "DirectoryViewRef directory_view" in file_header
    anchor = core[core.index("struct DirectoryAnchor {"):core.index("auto take_directory_anchor_identity(")]
    assert "DirectoryViewRef view" in anchor
    walk = body_after(core, "auto acquire_local_component_lookup(")
    assert "references.view = cwd_anchor.get()->view" in walk and "references.view = anchor->directory_view" in walk
    assert "admit_directory_view(nullptr, references.view.root(), references.view.directory(), authorization.credentials, true," in walk
    assert "!authorization.root_identity_view, references.membership.get()" in walk
    assert "confinement_root = root.data()" in walk and "confined_mount = references.view.root()->mount_owner" in walk
    assert "physical_path && confinement_root != nullptr && owner_directory == nullptr" in walk
    assert "if (!references.view &&" in walk and "if (references.view) return -EXDEV" in walk
    assert "references.view && !physical_path && link[0] == '/'" in walk and "task_root.data(), link, view_link" in walk
    assert "LookupHandleBuilder::set_directory_view(candidate, references.view)" in walk
    assert "if (file->directory_view) return -EINVAL" in body_after(core, "auto validate_directory_view_pin(")
    helper = body_after(core, "auto prepare_open_directory_view(")
    assert "DirectoryViewAdmission::IDENTITY_ONLY" in helper and "output = {}" in helper
    cwd = body_after(core, "auto change_directory_at(")
    assert "vfs_rebase_directory_view(*task, CREDENTIALS, lookup, context.view)" in cwd
    assert "args.candidate.get()->view = args.view" in cwd
    assert cwd.index("args.candidate.get()->view = args.view") < cwd.index("path_owner->cwd_anchor.swap(args.candidate)")
    assert "native_directory_member_path(root, directory, backend_path, nullptr)" in body_after(core, "auto directory_anchor_path(")
    assert "anchor->remote != nullptr || anchor->view" in body_after(core, "auto vfs_realpath(")
    for signature in ("auto open_read_file_as_impl(", "auto vfs_open_file_as("):
        assert "prepare_open_directory_view(path_context, REQUEST_CREDENTIALS, lookup, context.directory_view)" in body_after(core, signature)
    tests = (ROOT / "modules/kern/src/test/credential_ktest.cpp").read_text()
    assert "TmpfsDirectoryFilesAndCwdPreserveOwnedViews" in tests and "XfsDirectoryFilesAndCwdPreserveOwnedViews" in tests


def test_shared_open_stages_direct_file_and_serialized_view_before_publication():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    assert "open_at_for_task(task, dirfd, pathname, flags, mode, nullptr, true)" in body_after(core, "auto vfs_openat(")
    direct = body_after(core, "auto vfs_open_file(")
    assert "if (task == nullptr) return vfs_open_file_impl" in direct
    assert "open_at_for_task(task, AT_FDCWD, path, flags, mode, &file, false)" in direct
    opened = body_after(core, "static auto open_at_for_task(")
    assert "args.output == nullptr" in opened and "*args.output = file.release()" in opened
    assert "prepare_open_directory_view(*task, REQUEST_CREDENTIALS, lookup, context.directory_view)" in opened
    assert "&REQUEST_CREDENTIALS, &context.directory_view" in opened
    hybrid = body_after(core, "auto vfs_open_resolved_for_task(")
    assert hybrid.index("prepared_path.reset(new") < hybrid.index("switch (mount->fs_type)")
    assert hybrid.index("f->directory_view = *directory_view") < hybrid.index("vfs_cache_notify_register_open_file(f)") < hybrid.rindex("descriptor->install")
    assert "stable_lookup == nullptr || !stable_lookup->directory_view()" in hybrid
    serial = body_after(core, "auto open_local_serialized(")
    assert serial.index("std::unique_ptr<File, LocalWalkFileRelease> pin") < serial.index("publication_guard")
    assert serial.index("prepare_open_directory_view_locked(") < serial.index("return consume_lookup_locked(")
    staged = body_after(core, "auto prepare_open_directory_view_locked(")
    assert "if (pin || output) return -EINVAL" in staged
    assert "pin_directory_view_lookup(handle, false," in staged
    assert "source.root(), pin.get(), {}, output, false, true" in staged
    fixture = body_after(core, "auto vfs_selftest_directory_view_publication(")
    for token in ("vfs_fchdir(&task, child.number)", "BUSY + 2", "task.fd_table.size() == FD_COUNT",
                  "state.observed == state.closes", "vfs_rename(ROOT_PATH, MOVED)", "actual.st_ino == expected.st_ino"):
        assert token in fixture


def test_procfs_directory_links_capture_one_object_before_text_fallback():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    procfs = (ROOT / "modules/kern/src/vfs/fs/procfs.cpp").read_text()
    walk = body_after(core, "auto acquire_local_component_lookup(")
    walk = " ".join(walk.split())
    magic = walk[walk.index("ssize_t const READ = next.proc"):]
    assert "procfs::procfs_readlink_view_as" in magic
    assert magic.index("if (captured)") < magic.index("if (READ == 0)")
    branch = magic[magic.index("if (captured)"):magic.index("if (READ == 0)")]
    for token in ["references.view = std::move(captured)", "references.procfs_view_origin = std::move(next.proc)",
                  "result = admit_retained_view()", "EPOCH.confined_mount = confined_mount",
                  "copy_directory_lookup_path(references.view.directory()", "references.view.directory(), restart)",
                  "pending = remainder", "continue;"]:
        assert token in branch, token
    assert "local_walk_readlink" not in branch and "route_local_walk_path" not in branch
    assert "epoch.serialized && mount->fs_type != FSType::TMPFS && mount->fs_type != FSType::XFS" in body_after(core, "auto local_walk_bind(")
    read = body_after(procfs, "auto procfs_readlink_as_impl(")
    assert read.count("authorized_task_fd_retain(") == 1
    assert read.index("vfs_directory_view_from_file_for_task(") < read.index("if (result == 0 && !candidate_view && !directory_text)") < read.index("bool const HAS_PATH")
    assert "directory_view != nullptr ? &candidate_view : nullptr" in read
    assert read.index("int const VALID = revalidate_private_procfs") < read.index("*directory_view = std::move(candidate_view)")
    wrapper = body_after(procfs, "auto procfs_readlink_view_as(")
    assert "if (RESULT >= 0) directory_view = std::move(candidate)" in wrapper
    cwd = body_after(core, "auto vfs_getcwd_for_task(")
    assert cwd.index("anchor = owner->cwd_anchor") < cwd.index("candidate_view = anchor.get()->view")
    assert cwd.index("if (!CURRENT) return -EAGAIN") < cwd.index("*directory_view = std::move(candidate_view)")


def test_procfs_view_origin_is_owned_and_revalidated_at_native_commit():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    lookup = (ROOT / "modules/kern/src/vfs/lookup.cpp").read_text()
    for token in ["std::exchange(other.procfs_view_origin_, nullptr)", "std::exchange(other.release_procfs_view_origin_, nullptr)",
                  "std::exchange(other.procfs_view_generation_, 0)", "release_procfs_view_origin_(procfs_view_origin_)",
                  "procfs_view_origin_ = nullptr", "procfs_view_generation_ = 0"]:
        assert token in lookup, token
    validator = body_after(core, "auto validate_lookup_directory_view_locked(")
    assert validator.index("procfs::procfs_revalidate_open_result") < validator.index("admit_directory_view(")
    single = body_after(core, "auto consume_lookup_locked(")
    assert single.index("validate_lookup_directory_view_locked(handle)") < single.index("consumer(handle, context)")
    pair = body_after(core, "auto consume_lookup_pair_locked(")
    assert pair.index("validate_lookup_directory_view_locked(first)") < pair.index("consumer(first, second, context)")
    assert pair.index("validate_lookup_directory_view_locked(second)") < pair.index("consumer(first, second, context)")
    walk = body_after(core, "auto acquire_local_component_lookup(")
    assert "procfs::procfs_revalidate_open_result(references.procfs_view_origin.get(), references.procfs_view_generation)" in walk
    assert "references.procfs_view_origin.release(), release_procfs_lookup_cookie" in walk
    tests = (ROOT / "modules/kern/src/test/credential_ktest.cpp").read_text()
    assert "TmpfsProcfsMagicLinksPreserveOwnedViews" in tests and "XfsProcfsMagicLinksPreserveOwnedViews" in tests


def test_complete_filesystem_context_preserves_root_ownership_and_refuses_text_transfer():
    task = (ROOT / "modules/kern/src/platform/sched/task.cpp").read_text()
    capture = body_after(task, "auto Task::capture_filesystem_context(")
    assert capture.index("lock_irqsave()") < capture.index("candidate.root_anchor = owner->root_anchor")
    assert capture.index("candidate.cwd_anchor = owner->cwd_anchor") < capture.index("unlock_irqrestore(")
    assert capture.index("unlock_irqrestore(") < capture.index("output = std::move(candidate)")
    teardown = body_after(task, "void teardown_unpublished_process_resources(")
    assert "task->root_anchor = {}" in teardown and "task->cwd_anchor = {}" in teardown
    process = (ROOT / "modules/kern/src/syscalls_impl/process/process.cpp").read_text()
    assert process.count("child->root_anchor = filesystem_parent->root_anchor") == 2
    execute = (ROOT / "modules/kern/src/syscalls_impl/process/exec.cpp").read_text()
    assert "new_task->root_anchor = std::move(filesystem.root_anchor)" in execute
    plan = body_after(execute, "~RemoteExecPlan()")
    assert plan.index("filesystem.root_anchor = {}") < plan.index("actor->release()")
    exit_source = (ROOT / "modules/kern/src/syscalls_impl/process/exit.cpp").read_text()
    assert "DirectoryAnchorRef retired_root, retired" in exit_source
    assert "current_task->root_anchor.swap(retired_root)" in exit_source
    remote = (ROOT / "modules/kern/src/net/wki/remote_compute.cpp").read_text()
    encode = body_after(remote, "auto capture_submit_filesystem(")
    assert encode.index("if (filesystem.root_anchor) return false") < encode.index("candidate.root = filesystem.root")
    assert "candidate.cwd = preparation.cwd()" in encode
    apply = body_after(remote, "auto apply_submitted_task_identity(")
    assert apply.index("task->root_anchor") < apply.index("task->exchange_creation_mask(identity.umask)")
    scoped = body_after(remote, "ScopedSubmitVfsIdentity(ker::mod::sched::task::Task*")
    assert scoped.index("if (OWNED_ROOT) return") < scoped.index("saved_vfs_rules = std::move(")
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    fixture = body_after(core, "auto vfs_selftest_prepared_filesystem_context(")
    for token in ("auto complete = prepared.filesystem()", "auto copied = complete", "std::move(complete)",
                  "!complete.root_anchor && !complete.cwd_anchor", "prepared = {}", "MOUNT->open_files.load() == BUSY + 4",
                  "wki_remote_compute_selftest_filesystem_context(&copied)"):
        assert token in fixture
    snapshot = body_after(core, "auto vfs_selftest_filesystem_context_snapshot(")
    assert "owner.root_anchor.swap(retired_root)" in snapshot and "retirement.closes == 2" in snapshot


def test_filesystem_preparation_owns_both_anchors_without_publishing_task_state():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    header = (ROOT / "modules/kern/src/vfs/filesystem_context.hpp").read_text()
    prepare = body_after(core, "auto vfs_prepare_filesystem_context(")
    assert "PreparedFilesystemContext(const PreparedFilesystemContext&) = delete" in header
    assert "filesystem_.root_anchor && filesystem_.cwd_anchor" in header
    for capture in ("auto const REQUEST_CREDENTIALS = credentials", "auto retained = source",
                    "PreparedFilesystemContext candidate", "DirectoryViewRef root_view"):
        assert prepare.index(capture) < prepare.index("MutexGuard publication_guard")
    assert "REQUEST_CREDENTIALS, true, true, &membership_references" in re.sub(r"\s+", " ", prepare)
    assert prepare.index("NativeDirectoryMembershipReferences membership_references") < prepare.index("MutexGuard publication_guard")
    assert "root_view, false, true" in re.sub(r"\s+", " ", prepare)  # Root identity is not a fresh ancestor search.
    assert "candidate.filesystem_.root_anchor" in prepare and "candidate.filesystem_.cwd_anchor" in prepare
    assert "directory_anchor_path(root, physical, nullptr, &membership_references)" in prepare
    assert "directory_anchor_path(cwd, cwd_physical, nullptr, &membership_references)" in prepare
    assert "root.mount->fs_type" not in prepare and "cwd.tmp.target()" not in prepare
    assert "path_prefix_matches(cwd_physical.data(), physical.data(), ROOT_LENGTH)" in prepare
    assert prepare.index("}\n    // Source/credentials") < prepare.index("output = std::move(candidate)")
    for forbidden in ("vfs_alloc_fd", "vfs_getcwd", "get_current_task", "path_metadata_lock", "wki_", "vfs_open"):
        assert forbidden not in prepare
    pin = body_after(core, "auto make_directory_anchor_from_pin(")
    assert pin.index("if (output) return -EINVAL") < pin.index("output = allocate_directory_anchor()")
    assert pin.index("retain_mount_for_open_file") < pin.index("anchor.mount = mount")
    assert "xfs_acquire_lookup_at_file" in pin and "tmpfs_acquire_lookup" in pin
    fixture = body_after(core, "auto vfs_selftest_prepared_filesystem_context(")
    for token in ("-EACCES", "-ENAMETOOLONG", "-EPERM", "-ENOENT", "state.closes == 2", "state.unlocked",
                  "prepared.credentials(), prepared.directory_view(), prepared", "MOUNT->open_files.load()", "std::move(prepared)"):
        assert token in fixture


def test_native_mount_membership_uses_covered_identity_and_current_attachment():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    body = body_after(core, "auto directory_member_path_locked(")
    assert "MAX_LINKS = MOUNT_PATH_MAX / 2" in core
    assert "depth++ == NativeDirectoryMembershipReferences::MAX_LINKS" in body and "return -ELOOP" in body
    assert "references.capture(child_mount.get(), MOUNTS, covered)" in body
    assert body.index("references.current(MOUNTS)") < body.index("std::memcpy(buffer")
    assert "validate_native_mount_attachment(child_mount.get(), covered, MOUNTS)" in body
    attachment = body_after(core[core.index("auto validate_native_mount_attachment(") + 1:], "auto validate_native_mount_attachment(")
    assert "native_directory_member_path(nullptr, covered, covered_path)" in attachment
    assert "std::strcmp(covered_path.data(), child_path.data()) != 0" in attachment
    assert "std::strlen(child_path.data()) <= PARENT_LENGTH" in attachment
    assert attachment.count("find_mount_point(") == 1 and "selected_parent.get() != parent" in attachment
    assert "NAMESPACE != g_vfs_namespace_generation.load" in body and "MOUNTS != mount_table_generation_snapshot()" in body
    fixture = body_after(core, "auto vfs_selftest_native_mount_membership(")
    for expected in ('"/branch/child"', '"/branch/child/nested/deep"', '"/nested/deep"', "-ERANGE", "-ENOENT", "-EPERM", "-ENOTSUP"):
        assert expected in fixture
    assert "expect(deep.get(), replacement.get(), -EAGAIN" in fixture
    assert fixture.index('mount_child(BRANCH, "tmpfs"') < fixture.index("expect(child.get(), root.get(), -EAGAIN")
    assert "output == BEFORE && length == 0x55AA" in fixture
    assert "covered_child->refcount.load() == CHILD_REFS" in fixture and "covered_nested->refcount.load() == NESTED_REFS" in fixture
    assert "vfs_stat_resolved_beneath_as(task, user, BASE, LEAF, &denied, true, true) == -EACCES" in fixture


def test_cross_mount_context_admission_owns_proof_and_preserves_lookup_gate():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    prepare = body_after(core, "auto prepare_directory_view(")
    assert prepare.index("NativeDirectoryMembershipReferences membership_references") < prepare.index("admit_directory_view(")
    assert "namespace_already_locked ? locked_references : &membership_references" in prepare
    admit = body_after(core, "auto admit_directory_view(")
    assert admit.index("!references") < admit.index("publication_guard")
    assert "TMPFS_SEARCH == tmpfs::tmpfs_search_authority_generation()" in admit
    assert "XFS_SEARCH == xfs::xfs_search_authority_generation()" in admit
    member = body_after(core, "auto directory_view_member_path(")
    assert member.index("boundary->mount_owner == child->mount_owner") < member.index("!references") < member.index("directory_member_path_locked(")
    assert "native_directory_member_path(child, child, self, caller)" in member
    assert "static_cast" not in member
    guard = body_after(core, "auto validate_lookup_directory_view_locked(")
    assert "mount == view.root()->mount_owner" in guard
    bind = body_after(core, "auto local_walk_bind(")
    assert bind.index("epoch.confined_mount != nullptr") < bind.index("static_cast")
    fixture = body_after(core, "auto vfs_selftest_native_mount_membership(")
    assert "if (prepare_contexts || anchor_paths || cwd_recovery)" in fixture and "for (auto* directory : {leaf.get(), deep.get()})" in fixture
    for token in ("anchor_matches(prepared.root_anchor()", "anchor_matches(prepared.filesystem().cwd_anchor", "prepared.credentials(), prepared.directory_view(), prepared",
                  "vfs_narrow_directory_view(narrowed", "-ENAMETOOLONG", "state.closes == 2 && state.unlocked", "staged.root_anchor().get() == OLD"):
        assert token in fixture
    assert "scoped_stat.st_ino == expected_stat.st_ino" in fixture
    assert "TmpfsCrossMountContextOwnsTypedAnchors" in (ROOT / "modules/kern/src/test/credential_ktest.cpp").read_text()
    assert "XfsCrossMountContextOwnsTypedAnchors" in (ROOT / "modules/kern/src/test/credential_ktest.cpp").read_text()


def test_mount_membership_reuses_owned_edges_without_caching_authorization():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    owner = body_after(core, "struct NativeDirectoryMembershipReferences {")
    assert "MountRef child" in owner and "std::unique_ptr<File, LocalWalkFileRelease> covered" in owner
    assert "BLOCK_CAPACITY = 8" in owner and "Block* next = nullptr" in owner
    assert "while (block != nullptr)" in owner and "delete block" in owner
    assert "new (std::nothrow) Block{}" in owner and "if (next == nullptr) return -ENOMEM" in owner
    capture = body_after(owner, "auto capture(")
    assert capture.index("if (!current(expected))") < capture.index("entry.child.get() == mount") < capture.index("capture_mount_covered_directory(")
    assert capture.index("entry.covered.reset(covered)") < capture.index("observed != expected")
    assert "generation != 0 && generation != expected" in owner
    assert "mount_table_generation_snapshot() != expected" in owner
    assert "entry.child->retiring.load" in owner and "entry.covered->mount_owner->retiring.load" in owner
    fixture = body_after(core, "auto vfs_selftest_native_mount_membership(")
    for token in ("if (reuse_membership)", "MAX_LINKS + 3", "CHILD_REFS + 1", "NESTED_REFS + 1", "CHILD_MOUNT_REFS + 1",
                  "NESTED_MOUNT_REFS + 1", "references.fail_growth_for_test = true", "references.fail_growth_for_test = false",
                  "admit(user) == -EACCES", "admit(ROOT) == 0", "EPOCH + 1, empty", "-ENOMEM", "-EAGAIN", "spill-held"):
        assert token in fixture
    assert fixture.index("NativeDirectoryMembershipReferences references") < fixture.index("directory_member_path_locked(")
    tests = (ROOT / "modules/kern/src/test/credential_ktest.cpp").read_text()
    assert "TmpfsMountMembershipReusesEpochOwnedReferences" in tests and "XfsMountMembershipReusesEpochOwnedReferences" in tests


def test_lookup_membership_cookie_is_owned_and_consumer_access_is_read_only():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    header = (ROOT / "modules/kern/src/vfs/lookup.hpp").read_text()
    impl = (ROOT / "modules/kern/src/vfs/lookup.cpp").read_text()
    assert "mount_membership() const -> const void*" in header
    assert "std::exchange(other.mount_membership_, nullptr)" in impl
    assert "std::exchange(other.release_mount_membership_, nullptr)" in impl
    reset = body_after(impl, "void LookupHandle::reset()")
    assert reset.index("release_mount_membership_(mount_membership_)") < reset.index("mount_ref_.reset()")
    assert "mount_membership_ = nullptr" in reset and "release_mount_membership_ = nullptr" in reset
    install = body_after(impl, "auto LookupHandleBuilder::set_mount_membership(")
    assert install.index("retained == nullptr || release == nullptr || handle.mount_membership_ != nullptr") < install.index("handle.mount_membership_ = retained")
    assert "release(" not in install
    borrow = body_after(core, "auto borrow(MountPoint* mount")
    for forbidden in ("new ", "capture_mount_covered_directory", "vfs_retain_file", "vfs_put_file", "reset("):
        assert forbidden not in borrow
    assert "if (!current(expected)) return -EAGAIN" in borrow and "return -EAGAIN" in borrow
    access = body_after(core, "auto NativeDirectoryMembershipAccess::capture(")
    assert "recorder->capture" in access and "retained->borrow" in access
    validate = body_after(core[core.index("auto admit_directory_view("):], "auto validate_lookup_directory_view_locked(")
    assert "static_cast<const NativeDirectoryMembershipReferences*>" in validate
    assert "membership->current(handle.mount_generation())" in validate
    assert "directory_member_path_locked(covered, view.root()" in validate
    assert "handle.authorization().credentials, true, !handle.authorization().root_identity_view, membership" in re.sub(r"\s+", " ", validate)
    acquire = body_after(core, "auto vfs_acquire_directory_view_lookup(")
    assert acquire.index("LocalWalkReferences references") < acquire.index("MutexGuard publication_guard")
    assert "references.membership.get()" in acquire and "&references, mount, &retained_view" in " ".join(acquire.split())
    assert "set_mount_membership" not in acquire
    fixture = body_after(core, "auto vfs_selftest_native_mount_membership(")
    for token in ("if (lookup_membership)", "consume_lookup_locked(first", "consume_lookup_pair_locked(first, second",
                  "consume_lookup_pair_locked(second, first", "membership->fail_growth_for_test = true", "state.released == RELEASED",
                  "first.mount_membership() == nullptr", "moved.mount_membership() == COOKIE", "recording : {1U, 2U}",
                  "validate_lookup_directory_view_locked(valid) == -EACCES", "closing.closes == 2 && closing.unlocked",
                  "state.released == state.installed && state.unlocked", "prepare(root.get(), root.get(), DEEP"):
        assert token in fixture
    tests = (ROOT / "modules/kern/src/test/credential_ktest.cpp").read_text()
    assert "TmpfsLookupOwnsReadOnlyMountMembership" in tests and "XfsLookupOwnsReadOnlyMountMembership" in tests


def test_native_component_crossing_selects_covered_objects_and_owns_proof():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    crossing = body_after(core, "auto local_walk_cross_native_mount(")
    capture = crossing.index("references->capture(child, epoch.mount_generation, covered)")
    fallback = crossing.index("if (result == -ENOTSUP) return epoch.confined_mount != nullptr ? -EXDEV : RESOLVE_FAST_PATH_DECLINED")
    validate = crossing.index("validate_native_mount_attachment(child, covered")
    assert capture < fallback < validate
    assert "RESOLVE_FAST_PATH_DECLINED" not in crossing[validate:]
    assert crossing.index("local_walk_search(from, credentials)") < capture
    for token in ("covered->mount_owner != parent", "from.tmp->target() != tmpfs_root_for_mount(child)",
                  "from.xfs->mount() != root.mount()", "from.xfs->target() != root.target()",
                  "selected.target() != static_cast<tmpfs::TmpNode*>(covered->private_data)",
                  "selected.mount() != cover.mount()", "selected.target() != cover.target()",
                  "references->current(epoch.mount_generation)"):
        assert token in crossing, token
    assert 'tmpfs_acquire_lookup(credentials, from.tmp->target(), component' in crossing
    assert 'xfs_acquire_lookup_at_inode(credentials, from.xfs->mount(), from.xfs->target(), component' in crossing
    normalized = " ".join(crossing.split())
    assert 'Snapshot::bootstrap_root(), static_cast<tmpfs::TmpNode*>(covered->private_data), ".."' in normalized
    assert 'xfs_acquire_lookup_at_file(ker::mod::security::Snapshot::bootstrap_root(), covered, ".."' in normalized
    bind = body_after(core, "auto local_walk_bind(")
    assert bind.index("epoch.confined_mount != nullptr") < bind.index("local_walk_cross_native_mount(")
    assert "if (CROSS != RESOLVE_FAST_PATH_DECLINED) return CROSS" in bind
    assert "(parent == nullptr || CROSS_PARENT) && anchor == nullptr" in bind
    walk = body_after(core, "auto acquire_local_component_lookup(")
    assert walk.index("LocalWalkReferences local_references") < walk.index("LocalWalkEpoch EPOCH")
    assert '&current, "..", nullptr, next' in walk
    publish = walk[walk.rindex("LookupHandle candidate{};"):]
    assert publish.index("populate_lookup_path_fields(") < publish.index("set_mount_membership(") < publish.index("references.membership.release()") < publish.index("set_policy(")
    assert "return -" not in publish[publish.index("references.membership.release()"):]
    fixture = body_after(core, "auto vfs_selftest_native_mount_membership(")
    for token in ("if (native_crossing)", "native_crossing_ok &= DENIED", "actual.st_ino == 0x55AA",
                  "vfs_fchmod_file_as(covered_child.get(), ROOT, 0000)", "vfs_fchmod_file_as(covered_nested.get(), ROOT, 0000)",
                  "stat_path(CHILD, child.get(), user, 0)", "stat_path(CHILD_UP, nullptr, user, -EACCES)",
                  "serialized ? &references : nullptr", "moved.mount_membership() == COOKIE",
                  "from_child.mount_membership() != nullptr", "covered_nested->refcount.load() == NESTED_REFS"):
        assert token in fixture, token
    tests = (ROOT / "modules/kern/src/test/credential_ktest.cpp").read_text()
    assert "TmpfsNativeMountCrossingUsesCoveredObjects" in tests and "XfsNativeMountCrossingUsesCoveredObjects" in tests


def test_typed_anchor_path_validates_identity_and_requires_outer_cross_mount_proof():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    path = body_after(core, "auto directory_anchor_path(")
    assert path.index("CROSS_MOUNT && !references") < path.index("static_cast")
    assert "directory->mount_owner != mount" in path
    assert "anchor.tmp.target() != static_cast<tmpfs::TmpNode*>(directory->private_data)" in path
    assert "anchor.xfs.mount() != identity.mount() || anchor.xfs.target() != identity.target()" in path
    assert "directory_member_path_locked(directory, root" in path
    assert "native_directory_member_path(root, directory, backend_path, nullptr)" in path
    assert "static_cast<tmpfs::TmpNode*>(root->private_data)" not in path
    assert "references.current(MOUNTS)" in path
    assert path.index("NAMESPACE != g_vfs_namespace_generation.load") < path.index("out = candidate")
    for forbidden in ("new ", "vfs_retain_file", "vfs_put_file", "MutexGuard", "get_current_task"):
        assert forbidden not in path
    prepare = body_after(core, "auto vfs_prepare_filesystem_context(")
    assert prepare.index("NativeDirectoryMembershipReferences membership_references") < prepare.index("MutexGuard publication_guard")
    assert prepare.index("directory_anchor_path(cwd, cwd_physical") < prepare.index("path_prefix_matches(cwd_physical.data()")
    fixture = body_after(core, "auto vfs_selftest_native_mount_membership(")
    for token in ("if (anchor_paths)", "recover(NativeDirectoryMembershipAccess(), -EXDEV)",
                  "NativeDirectoryMembershipReferences*>(&empty), -EAGAIN", "recover(references, 0)",
                  "references.fail_growth_for_test = true", "actual == BEFORE", "cwd_anchor.view = wrong",
                  "cwd_anchor.view = original", "vfs_chmod(expected, 0000)", "typed anchor path result"):
        assert token in fixture, token
    tests = (ROOT / "modules/kern/src/test/credential_ktest.cpp").read_text()
    assert "TmpfsTypedAnchorPathAcrossNativeMounts" in tests and "XfsTypedAnchorPathAcrossNativeMounts" in tests


def test_cwd_recovery_retains_proof_outside_publication_and_rejects_before_capture():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    cwd = body_after(core, "auto vfs_getcwd_for_task(")
    assert cwd.index("NativeDirectoryMembershipReferences membership") < cwd.index("MutexGuard> publication_guard")
    refusal = cwd.index("remote_credentials == nullptr && directory_view == nullptr && crosses_mount(owner->cwd_anchor)")
    assert refusal < cwd.index("anchor = owner->cwd_anchor")
    assert cwd.index("owner->path_metadata_lock.unlock_irqrestore(IRQF);", cwd.index("anchor = owner->cwd_anchor")) < cwd.index("cwd_recovery_test_hook(owner)")
    assert "crosses_mount(anchor) && anchor.get()->remote == nullptr" in cwd
    assert "if (CROSS_NATIVE) publication_guard.emplace(g_vfs_namespace_publication_mutex)" in cwd
    assert "CROSS_NATIVE ? &membership : nullptr" in cwd
    assert cwd.index("directory_anchor_path(") < cwd.index("uint64_t const CHECK_IRQF") < cwd.index("if (!CURRENT) return -EAGAIN") < cwd.index("std::memcpy(buf")
    assert "is_locked(" not in cwd
    proc = body_after(core, "auto readlink_procfs_lookup_as(")
    assert proc.index("procfs::procfs_readlink_as(") < proc.index("MutexGuard publication_guard")
    fixture = body_after(core, "auto vfs_selftest_native_mount_membership(")
    for token in ("if (cwd_recovery)", "owned cwd recovery result", "!getcwd(-ERANGE, 2)",
                  "observer.cwd_anchor.get()->refs.load() == REFS", "procfs::procfs_readlink_as(origin.get()",
                  "procfs::procfs_readlink_view_as(origin.get()", "current.retained_on_withdrawal &= current.closes == 0",
                  "retired.swap(owner->cwd_anchor)", "state.withdrawals == 1", "state.closes == 2 && state.unlocked",
                  "!expect_context_cwd_failure(removed_context, -ENOENT)", "!expect_context_cwd_failure(staged, -EAGAIN)"):
        assert token in fixture, token
    tests = (ROOT / "modules/kern/src/test/credential_ktest.cpp").read_text()
    assert "TmpfsCwdRecoveryOwnsCrossMountProof" in tests and "XfsCwdRecoveryOwnsCrossMountProof" in tests


def test_scoped_native_walk_proves_selected_objects_and_borrows_open_membership():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    for file, prefix in (("tmpfs.cpp", "tmpfs"), ("xfs/xfs_vfs.cpp", "xfs")):
        native = (ROOT / "modules/kern/src/vfs/fs" / file).read_text()
        body = body_after(native, "auto " + prefix + "_lookup_directory_path_beneath(")
        assert body.index(prefix + "_validate_lookup_locked(lookup)") < body.index("selected = target")
        assert "target : lookup.parent()" in body
        assert "capacity, true, nullptr" in body or "capacity, nullptr" in body
        for forbidden in ("new ", "pin_directory", "vfs_retain_file", "vfs_put_file"):
            assert forbidden not in body
    validate = body_after(core, "auto validate_lookup_directory_view_locked(")
    assert validate.index("admit_directory_view(") < validate.index("tmpfs_lookup_directory_path_beneath(")
    assert "xfs_lookup_directory_path_beneath" in validate and "lookup.mount() != mount->private_data" in validate
    assert "NativeDirectoryMembershipAccess references(membership)" in validate
    assert validate.index("references.capture(") < validate.index("validate_native_mount_attachment(") < validate.index("directory_member_path_locked(")
    for forbidden in ("new ", "vfs_retain_file", "vfs_put_file", "pin_directory", "find_mount_point"):
        assert forbidden not in validate
    walk = body_after(core, "auto acquire_local_component_lookup(")
    assert "references.view = *initial_view" in walk
    assert "directory_anchor_path(*cwd_anchor.get(), path, nullptr, references.membership.get())" in walk
    bind = body_after(core, "auto local_walk_bind(")
    assert "if (epoch.confined_mount != nullptr) return -EXDEV" in bind
    locked = body_after(core, "auto prepare_open_directory_view_locked(")
    assert "static_cast<const NativeDirectoryMembershipReferences*>(lookup.mount_membership())" in locked
    assert "output, false, true, membership" in locked
    fixture = body_after(core, "auto vfs_selftest_native_mount_membership(")
    for token in ("if (scoped_walk)", "outside_selection", "consume(outside_selection, inside_selection, user, -EPERM)",
                  "open_local_serialized(&observer", "vfs_fchdir(&observer", "CREATED_FD == -EROFS", "DENIED == -EACCES",
                  "actual.st_ino == seed_stat.st_ino", "calls == 0", "covered_nested->refcount.load() == NESTED_REFS"):
        assert token in fixture, token


def test_statvfs_admits_retained_caller_lookup_and_stages_outputs():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    public = body_after(core, "auto vfs_statvfs(")
    assert public.index("statvfs_for_task(*task, path, buf)") < public.index("resolve_task_path_raw_impl(")
    query = body_after(core, "auto statvfs_for_task(")
    assert query.count("task.credentials()") == 1
    assert query.index("REQUEST_CREDENTIALS = task.credentials()") < query.index("for (unsigned attempt") < query.index("vfs_acquire_lookup(")
    assert "LookupIntent::STAT, LookupFollowPolicy::FOLLOW_FINAL" in query
    assert "&REQUEST_CREDENTIALS" in query
    assert query.index("wki_remote_vfs_stat_at(") < query.index("MutexGuard publication_guard")
    assert query.index("wki_remote_vfs_stat(mount->private_data") < query.index("MutexGuard publication_guard")
    assert query.index("procfs_fill_stat(") < query.index("MutexGuard publication_guard")
    assert "procfs_revalidate_metadata_result" in query and "stat_owner_lookup(handle, &args.metadata)" in query
    assert query.index("if (valid != 0) return valid") < query.index("statvfs_for_mount(")
    assert "consume_lookup_locked(lookup, false, consume, &context, &task, &REQUEST_CREDENTIALS)" in query
    assert "lookup_request_context_matches(lookup, &task, REQUEST_CREDENTIALS)" in query
    assert "current_mount.get() != mount" in query and "lookup.mount_generation()" in query
    assert "handle.mount_policy().read_only" in query and "handle.mount_policy().nosuid" in query
    assert query.index("consume_lookup_locked(") < query.index("*output = context.stats")
    for forbidden in ("get_current_task", "resolve_task_path_raw", "vfs_open", "memset(output", "*output = {}"):
        assert forbidden not in query
    file = body_after(core, "auto vfs_fstatvfs_file(")
    assert file.index("Statvfs candidate{}") < file.index("statvfs_for_file(file, &candidate)") < file.index("if (RESULT == 0) *buf = candidate")
    fixture = body_after(core, "auto vfs_selftest_statvfs_authorization(")
    for token in ("query(SEED, -EACCES)", "query(LINK, -EACCES)", "queries == QUERIES", "task.credentials() == user",
                  "vfs_fstatvfs_file(&unsupported, &unchanged) == -ENOSYS", "/proc/no-such-statvfs-object", "std::memcmp"):
        assert token in fixture, token
    scoped = body_after(core, "auto vfs_selftest_native_mount_membership(")
    for token in ("if (filesystem_stats)", "statvfs_for_task(observer, name, &stats)", "race.attempts == 4 && race.queries == 0",
                  "query(\"seed\", -EAGAIN)", "RENAMED_STATS && RESTORED == 0", "stats.f_flag == expected_stats.f_flag"):
        assert token in scoped, token
    tests = (ROOT / "modules/kern/src/test/credential_ktest.cpp").read_text()
    assert "TmpfsStatvfsUsesCallerAndPreservesDeniedOutput" in tests and "XfsStatvfsUsesScopedNativeCwd" in tests


def test_owned_root_getcwd_uses_native_metadata_and_intersects_views():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    cwd = body_after(core, "auto vfs_getcwd_for_task(")
    assert cwd.index("remote_credentials == nullptr && owner->root_anchor") < cwd.index("root_anchor = owner->root_anchor")
    assert cwd.index("root_anchor = owner->root_anchor") < cwd.index("cwd_recovery_test_hook(owner)")
    for token in ("directory_anchor_path(*root_anchor.get()", "directory_member_path_locked(source_view.directory(), process_view.root()",
                  "REQUEST_CREDENTIALS, true, false, &membership", "source_view.policy().restricted_by(process_view.policy())",
                  "root_anchor.get() == owner->root_anchor.get()", "text = physical.data()"):
        assert token in cwd, token
    fixture = body_after(core, "auto vfs_selftest_owned_root_absolute_walk(")
    for token in ("if (cwd_metadata)", "owned root cwd metadata result", "!recover(-ERANGE, 4)",
                  "variant < 3", "canonical->root_anchor.swap(retired)", "recover(-EAGAIN, MAX_PATH_LEN, &unchanged_view)",
                  "recover(-ENOTSUP, MAX_PATH_LEN, nullptr, false)", "output.policy().nosuid"):
        assert token in fixture, token
    mixed = body_after(core, "auto vfs_selftest_native_mount_membership(")
    assert "observer.root_anchor = prepared.root_anchor()" in mixed and "owned_view.directory() == directory" in mixed
    registrations = (ROOT / "modules/kern/src/test/vfs_ktest.cpp").read_text()
    assert "TmpfsGetcwdUsesOwnedProcessRoot" in registrations and "XfsGetcwdUsesOwnedProcessRoot" in registrations


def test_owned_root_visible_consumers_pair_capture_and_stage_publication():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    capture = body_after(core, "auto capture_lookup_root(")
    assert capture.index("path_metadata_lock.unlock_irqrestore") < capture.index("output = std::move(candidate)")
    visible = body_after(core, "auto lookup_visible_path_locked(")
    assert visible.index("directory_anchor_identity(captured.anchor)") < visible.index("directory_anchor_path(*captured.anchor.get()") < visible.index("const char* visible =")
    public = body_after(core, "auto vfs_realpath(")
    assert public.index("if (owned_root || retained_object_cwd)") < public.index("resolve_task_path_raw_impl(")
    chdir = body_after(core, "auto change_directory_at(")
    assert chdir.index("lookup_visible_path_locked(handle, args.root") < chdir.index("path_owner->path_metadata_lock.lock_irqsave()")
    assert chdir.index("handle.authorization().root_anchor_identity") < chdir.index("path_owner->cwd_anchor.swap")
    fixture = body_after(core, "auto vfs_selftest_owned_root_absolute_walk(")
    for token in ("if (visible_paths)", "owned root realpath", "owned root chdir", "vfs_fchdir(request, FD)",
                  'realpath("file", "/cwd/file")', 'change(FD, "..", "", -EPERM)', "STALE == -EAGAIN && !unchanged.anchor"):
        assert token in fixture, token
    tests = (ROOT / "modules/kern/src/test/vfs_ktest.cpp").read_text()
    assert "TmpfsRealpathAndChdirUseOwnedRoot" in tests and "XfsRealpathAndChdirUseOwnedRoot" in tests


def test_procfs_owned_root_and_scoped_fd_metadata_preserve_subject_boundary():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    proc = (ROOT / "modules/kern/src/vfs/fs/procfs.cpp").read_text()
    root = body_after(core, "auto vfs_getroot_for_task(")
    assert root.index("caller == nullptr && owner->root_anchor") < root.index("anchor = owner->root_anchor")
    assert root.index("directory_anchor_path(*anchor.get()") < root.index("if (!CURRENT) return -EAGAIN") < root.index("*directory_view = std::move(candidate)")
    scope = body_after(core, "auto vfs_scope_directory_view_for_task(")
    for token in ("NativeDirectoryMembershipReferences membership", "validate_directory_view_pin(file)",
                  "REQUEST_CREDENTIALS, true, false, &membership", "retained.policy().restricted_by(process_view.policy())",
                  "anchor.get() == owner->root_anchor.get()"):
        assert token in scope, token
    link = body_after(proc, "auto procfs_readlink_as_impl(")
    assert link.index("authorize_private_procfs(") < link.index("vfs_getroot_for_task(")
    assert link.index("vfs_directory_view_from_file_for_task(") < link.index("revalidate_private_procfs(") < link.index("*directory_view = std::move(candidate_view)")
    fixture = body_after(core, "auto vfs_selftest_owned_root_absolute_walk(")
    for token in ("if (procfs_roots)", 'procfs_lookup_path_as("self/root"', "owned proc root result", "OUTSIDE ? -EPERM : 0",
                  "root_link(-EACCES, true", "root_link(-ENOTSUP, false", "root_link(-EAGAIN, true)", "procfs_revalidate_readlink_result"):
        assert token in fixture, token
    tests = (ROOT / "modules/kern/src/test/vfs_ktest.cpp").read_text()
    assert "TmpfsProcfsRootAndFdViewsRespectOwnedRoot" in tests and "XfsProcfsRootAndFdViewsRespectOwnedRoot" in tests


def test_procfs_plain_directory_pins_exact_native_file_beneath_owned_root():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    capture = body_after(core, "auto vfs_directory_view_from_file_for_task(")
    for token in ("directory->directory_view", "vfs_scope_directory_view_for_task(", "NativeDirectoryMembershipReferences membership",
                  "directory_member_path_locked(directory, root_view.root()", "MountOpenFileReservation reservation(mount)",
                  'directory->private_data), "", &lookup)', 'xfs_acquire_lookup_at_file(REQUEST_CREDENTIALS, directory, "", &lookup)',
                  "lookup.mount() != static_cast<xfs::XfsMountContext*>(mount->private_data)", "lookup, &result, false)",
                  "reservation.attach(pin.get())", "DirectoryView(root_view.root(), pin.get(), root_view.policy())"):
        assert token in capture, token
    for forbidden in ("directory->vfs_path", "vfs_open_file(", "resolve_symlink", "metadata_cache", "directory->mount_policy ="):
        assert forbidden not in capture
    assert capture.index("directory_member_path_locked(") < capture.index("tmpfs_pin_directory_lookup(")
    assert capture.index("if (!CURRENT) return -EAGAIN") < capture.index("output = std::move(candidate)")
    fixture = body_after(core, "auto vfs_selftest_owned_root_absolute_walk(")
    for token in ("if (plain_directories)", "plain proc directory variant", "ERRORS{0, -EPERM, -EBADF, -ENOTSUP}",
                  "query(-EAGAIN)", "file->pos == 17", "!ordinary->directory_view"):
        if token == "!ordinary->directory_view":
            assert token in body_after(core, "auto vfs_selftest_native_mount_membership(")
        else: assert token in fixture, token
    tests = (ROOT / "modules/kern/src/test/vfs_ktest.cpp").read_text()
    assert "TmpfsProcfsPlainDirectoryUsesRetainedInode" in tests and "XfsProcfsPlainDirectoryUsesRetainedInode" in tests


def test_procfs_directory_text_uses_exact_inode_and_process_root():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    capture = body_after(core, "auto vfs_directory_path_for_task(")
    for token in ("DirectoryAnchorRef anchor", "DirectoryViewRef source", "NativeDirectoryMembershipReferences membership",
                  "return DIRECTORY_PATH_LEGACY", "if (caller == nullptr)", "source = directory->directory_view",
                  "directory_anchor_path(*anchor.get()", "admit_directory_view(nullptr, source.root()",
                  "REQUEST_CREDENTIALS, true, false, &membership)", "directory_member_path_locked(selected, root_view.root()",
                  "text == owner->root && anchor.get() == owner->root_anchor.get()", "if (staged_length + 1 > capacity) return -ERANGE"):
        assert token in capture, token
    for token in ("vfs_open_file(", "directory->vfs_path", "new (", "pin_directory", "metadata_cache"):
        assert token not in capture, token
    assert capture.index("if (caller == nullptr)") < capture.index("anchor = owner->root_anchor") < capture.index("source = directory->directory_view")
    assert capture.index("if (!CURRENT) return -EAGAIN") < capture.index("std::memcpy(buffer")
    proc = (ROOT / "modules/kern/src/vfs/fs/procfs.cpp").read_text()
    read = body_after(proc, "auto procfs_readlink_as_impl(")
    assert read.index("vfs_directory_path_for_task(") < read.index("if (result == 0 && !candidate_view && !directory_text)")
    assert "directory_text = result != ker::vfs::DIRECTORY_PATH_LEGACY" in read
    assert read.index("revalidate_private_procfs(") < read.index("std::memcpy(buf, staged.data()")
    fixture = body_after(core, "auto vfs_selftest_owned_root_absolute_walk(")
    for token in ('text_query(0, "/text-cwd")', 'text_query(0, "/text-cwd", 2)', 'text_query(0, "/text-cwd", 0)',
                  'text_query(-ENOTSUP', 'text_query(-EAGAIN)', 'text_query(-EACCES', 'bool const NARROW = text_query(0)',
                  'file->directory_view.get() == OLD_VIEW'):
        assert token in fixture, token
    tests = (ROOT / "modules/kern/src/test/vfs_ktest.cpp").read_text()
    assert "TmpfsProcfsDirectoryTextUsesRetainedInode" in tests and "XfsProcfsDirectoryTextUsesRetainedInode" in tests


def test_procfs_mount_reads_capture_current_root_without_shared_cache():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    capture = body_after(core, "auto vfs_capture_root_metadata(")
    for token in ("FilesystemRootMetadata candidate", "candidate.owner_ = owner", "candidate.text_ = owner->root",
                  "candidate.anchor_ = owner->root_anchor", "directory_anchor_path(*candidate.anchor_.get()",
                  "candidate.namespace_generation_", "candidate.mount_generation_", "view.policy().read_only", "view.policy().nosuid"):
        assert token in capture, token
    assert capture.index("FilesystemRootMetadata candidate") < capture.index("MutexGuard guard")
    assert capture.index("if (!vfs_root_metadata_current(task, candidate))") < capture.index("output = std::move(candidate)")
    for forbidden in ("cwd_anchor", "get_mount_snapshot_at", "vfs_open_file(", "resolve_symlink"):
        assert forbidden not in capture
    current = body_after(core, "auto vfs_root_metadata_current(")
    for token in ("captured.owner_ != owner", "captured.namespace_generation_", "captured.mount_generation_",
                  "captured.text_ == owner->root", "captured.anchor_.get() == owner->root_anchor.get()"):
        assert token in current, token
    proc = (ROOT / "modules/kern/src/vfs/fs/procfs.cpp").read_text()
    generation = body_after(proc, "auto generate_mounts_for_task(")
    assert generation.index("vfs_capture_root_metadata(") < generation.index("get_mount_snapshot_at(") < generation.index("vfs_root_metadata_current(")
    for forbidden in ("MutexGuard", "copy_proc_filesystem_root", "get_current_task", "get_mount_at("):
        assert forbidden not in generation
    for token in ("INSIDE_ROOT && root.read_only()", "INSIDE_ROOT && root.nosuid()"):
        assert token in generation
    read = body_after(proc, "auto procfs_read_mounts_as(")
    assert read.index("authorize_diagnostic(") < read.index("generate_mounts_for_task(") < read.index("std::memcpy(")
    for forbidden in ("data.content =", "data.content_len =", "MutexGuard", "new ("):
        assert forbidden not in read
    seek = body_after(proc, "auto procfs_lseek_mounts_as(")
    assert seek.index("generate_mounts_for_task(") < seek.index("MutexGuard content_guard") < seek.index("file->pos = POSITION")
    assert "if (POSITION < 0) return -EINVAL" in seek
    fixture = body_after(core, "auto vfs_selftest_owned_root_absolute_walk(")
    for token in ("if (mount_metadata)", 'std::strcpy(data->content, "stale")', "read_full(&global_reader",
                  "bool const CAPTURE_STALE", "bool const ROWS_STALE", "bool const NAMESPACE_STALE",
                  "procfs::procfs_lseek_mounts_as", "!vfs_root_metadata_current(request, observation)", "FILES_BEFORE_CHILD"):
        assert token in fixture, token
    tests = (ROOT / "modules/kern/src/test/vfs_ktest.cpp").read_text()
    assert "TmpfsProcfsMountsUseCurrentRootAndReader" in tests and "XfsProcfsMountsUseCurrentRootAndReader" in tests


def test_directory_transfer_grants_bind_snapshot_lifetime_and_deferred_admission():
    remote = (ROOT / "modules/kern/src/net/wki/remote_vfs.cpp").read_text()
    server = (ROOT / "modules/kern/src/net/wki/dev_server.cpp").read_text()
    wki = (ROOT / "modules/kern/src/net/wki/wki.cpp").read_text()
    wire = (ROOT / "modules/kern/src/net/wki/wire.hpp").read_text()
    assert "WKI_VFS_FEATURE_DIRECTORY_GRANT = 1024" in wire
    for source, marker in ((server, "auto is_vfs_op("), (wki, "auto message_uses_vfs_export_admission("),
                           (wki, "auto admit_vfs_op(")):
        assert "OP_VFS_DIRECTORY_GRANT" in body_after(source, marker)
    retain = body_after(remote, "auto retain_directory_grant(")
    assert retain.index("s_vfs_lock.unlock()") < retain.index("wki_dev_server_retain_vfs_grant_source(")
    assert retain.index("directory_grant_credentials != RECORD") < retain.index("vfs_retain_file(")
    assert "touch_remote_fd" not in retain
    native = body_after(remote, "auto wki_remote_vfs_adopt_directory_grant_native(")
    positions = [native.index(token) for token in ("source.submission_id != grant.submission_id", "map_compute_identity(",
                  "retain_directory_grant(", "vfs_narrow_directory_view(", "claim_directory_grant(", "output.swap(")]
    assert positions == sorted(positions)
    owner = body_after(remote, "auto directory_grant_owner(")
    assert owner.index("vfs_pin_directory_view_as(") < owner.index("publish_directory_adoption(") < owner.index("response = result")
    client = body_after(remote, "auto directory_grant_client_rpc(")
    assert client.index("WKI_VFS_FEATURE_DIRECTORY_GRANT") < client.index("reserve_exec_cancellation(") < client.index("OP_VFS_DIRECTORY_GRANT")
    assert client.index("wki_vfs_directory_grant_response_valid(") < client.index("*output = preparation.publish()")
    assert "preparation.cancel()" in client and "&identity" in client
    assert client.count("OP_VFS_DIRECTORY_GRANT") == 1  # No retry of a claimed transfer.
    pin = body_after(remote, "auto wki_remote_vfs_pin_directory(")
    assert "file->mount_policy = directory->mount_policy" in pin
    fixture = body_after(remote, "auto wki_remote_vfs_selftest_directory_grant_lifetime(")
    for token in ("for (bool disk : {false, true})", "NO_PARTIAL", "VFS_DIRECTORY_GRANTS_PER_CONSUMER", "-EMFILE", "-EROFS",
                  "-EALREADY", "-EACCES", "UINT64_MAX", "vfs_rename(EXPORT, MOVED)", "worker.credentials() != caller"):
        assert token in fixture, token


def test_compute_queues_retain_one_authenticated_peer_identity():
    source = (ROOT / "modules/kern/src/net/wki/remote_compute.cpp").read_text()
    capture = body_after(source, "auto compute_peer_identity(")
    for token in ("PeerState::CONNECTED", "peer.auth_session.active", "peer.remote_boot_epoch", "peer.auth_session.generation",
                  "peer.auth_session.session_id", "peer.auth_session.policy", "peer.capabilities & local_capabilities"):
        assert token in capture, token
    for forbidden in (".lock(", "lock_irqsave", "new ", "wki_wait"):
        assert forbidden not in capture
    admission = body_after(source, "auto wki_remote_compute_admit_rx(")
    assert "queue_compute_rx_work(type, hdr, payload, copy_len, rx_channel, rx_channel_generation)" in admission
    rx = body_after(source, "auto queue_compute_rx_work(")
    assert rx.index("capture_compute_peer_identity(") < rx.index("s_compute_rx_lock.lock()")
    assert rx.index("slot.identity = IDENTITY") < rx.index("shard.slot_indices.at(shard.tail)")
    assert "s_compute_lock" not in rx
    worker = body_after(source, "void compute_rx_worker_loop(")
    assert worker.index("compute_rx_identity_matches(") < worker.index("authorize_user_signal(")
    queue = body_after(source, "void queue_admitted_task_submit(")
    assert queue.index("compute_submit_admission_is_current_locked(") < queue.index("new (") < queue.index("pending.session = SESSION")
    fragment = body_after(source, "void handle_task_submit_fragment_work(")
    assert fragment.index("capture_compute_submit_session_locked(") < fragment.index("s_fragmented_task_submit_lock.lock()")
    assert "candidate.session.identity == SESSION.identity" in fragment and "candidate.session = SESSION" in fragment
    assert "queue_admitted_task_submit(&hdr, completed_payload, fragment.total_len, SESSION)" in fragment
    assert "detail::handle_task_submit(" not in fragment
    work = body_after(source, "void handle_task_submit_work(")
    assert "wki_peer_remote_boot_epoch_snapshot" not in work
    assert "uint32_t const SOURCE_BOOT = session.identity.boot_epoch" in work
    assert ".boot = session.identity.boot_epoch" in work
    assert "compute_submit_admission_is_current_locked(session)" in work
    for function in ("auto authorize_user_signal(", "auto map_group_caller(", "auto authorize_group_signal("):
        decision = body_after(source, function)
        assert "work.identity.policy" in decision and "work.identity.boot_epoch" in decision
        assert "wki_peer_remote_boot_epoch_snapshot" not in decision
    fixture = body_after(source, "auto wki_remote_compute_selftest_admitted_peer_identity(")
    for token in ("MIXED_DENIED", "KEY_ONLY", "ORIGIN.boot != 17", "handle_task_submit_fragment_work(",
                  "release_fragmented_task_submits(", "work.identity != CAPTURED"):
        assert token in fixture, token


def test_native_directory_grants_publish_exact_authority_and_release_after_unlock():
    remote = (ROOT / "modules/kern/src/net/wki/remote_vfs.cpp").read_text()
    issue = body_after(remote, "auto wki_remote_vfs_issue_native_directory_grant(")
    tokens = ("REQUEST_CREDENTIALS = credentials", "wki_remote_vfs_find_export_snapshot(",
              "vfs_pin_directory_resolved_beneath_as(", "root->mount_owner->dev_id != resource.backing_dev_id",
              "vfs_directory_path_beneath_directory(source.root(), root", "vfs_narrow_directory_view(",
              "vfs_pin_directory_view_as(", "credential_record::encode(REQUEST_CREDENTIALS)",
              "prepared.extract(0)", "s_vfs_lock.lock()", "native_grant_resource_current_locked(",
              "g_native_directory_grants.size() >= VFS_DIRECTORY_GRANTS_PER_CONSUMER",
              "take_directory_grant_id(", "g_native_directory_grants.insert(", "s_vfs_lock.unlock()",
              "candidate.release()", "output = grant", "*staging = cleanup_file.release()")
    positions = [issue.index(token) for token in tokens]
    assert positions == sorted(positions)
    assert "task.credentials()" not in issue and "g_remote_fds" not in issue
    cancel = body_after(remote, "auto cancel_native_directory_grant(")
    assert cancel.index("g_native_directory_grants.extract(") < cancel.index("s_vfs_lock.unlock()") < cancel.index("vfs_put_file_result(")
    current = body_after(remote, "auto native_grant_resource_current_locked(")
    for token in ("grant.source_boot_epoch != g_wki.local_boot_epoch", "revision != g_vfs_export_revision",
                  "exp.active", "exp.resource_id == grant.resource_id", "exp.resource_incarnation == grant.owner_incarnation.resource_incarnation",
                  "exp.publication_revision == revision"):
        assert token in current, token
    source = body_after(remote, "auto directory_grant_source_current_locked(")
    assert "binding == nullptr && native_grant_resource_current_locked(" in source
    assert "binding != nullptr && grant_binding_matches(" in source
    fixture = body_after(remote, "auto wki_remote_vfs_selftest_native_directory_grant(")
    for token in ("for (bool disk : {false, true})", "set_resource_active(false)", "-ENOTCONN", "-EACCES", "-EALREADY",
                  "-EROFS", "-EPERM", "-EMFILE", "directory_grant_adoption_test_hook", "vfs_rename(EXPORT, MOVED)",
                  "g_native_directory_grants.empty()", "worker.credentials() != caller"):
        assert token in fixture, token


def test_grant_rpc_preparation_survives_abandoned_executor_waits():
    source = (ROOT / "modules/kern/src/net/wki/remote_vfs.cpp").read_text()
    client = body_after(source, "auto directory_grant_client_rpc(")
    tokens = ("PinPreparation preparation(state, PinPurpose::DIRECTORY)", "OP_VFS_FEATURES", "reserve_exec_cancellation(",
              "preparation.own_cancellation(SLOT, TOKEN)", "OP_VFS_DIRECTORY_GRANT", "preparation.cancel()",
              "grant_output = result.grant", "*output = preparation.publish()")
    positions = [client.index(token) for token in tokens]
    assert positions == sorted(positions)
    assert "cancel_remote_exec_view(" not in client and "ProxyOpenRefGuard" not in client
    guard = body_after(source, "class PinPreparation {")
    detach = body_after(guard, "auto detach_locked(")
    for token in ("record.phase == ExecCancellationPhase::OWNED", "record.proxy == retired.proxy", "record.token == token_",
                  "record.directory", "record.phase = ExecCancellationPhase::PENDING", "slot_ = VFS_EXEC_CANCEL_SLOT_NONE"):
        assert token in detach, token
    cancel = body_after(guard, "void cancel(")
    assert cancel.index("detach_locked()") < cancel.index("s_vfs_lock.unlock()") < cancel.index("drain(retired)")
    assert "vfs_proxy_send_and_wait" not in guard and "cancel_remote_exec_view" not in guard
    cleanup = body_after(source, "void wki_remote_vfs_cleanup_for_task(")
    assert cleanup.index("finish_or_quiesce_waiter(") < cleanup.index("PinPreparation::cleanup_for_task(pid)")
    fixture = body_after(source, "auto wki_remote_vfs_selftest_directory_grant_preparation(")
    for token in ("FIRST_PID", "OTHER_PID", "wki_remote_vfs_cleanup_for_task(FIRST_PID)",
                  "g_pin_private_files_freed.load() != FILES + 5", "success.publish()",
                  "binding_retirement_confirmed = true", "vfs_put_file_result(published.release())", "CLOSED != -ENOTCONN"):
        assert token in fixture, token


def test_all_retained_pin_constructors_own_preparation_across_waits():
    source = (ROOT / "modules/kern/src/net/wki/remote_vfs.cpp").read_text()
    directory = body_after(source, "auto wki_remote_vfs_pin_directory(")
    tokens = ("PinPreparation preparation(state, PinPurpose::DIRECTORY)", "preparation.status()", "lane_guard.reset()",
              "OP_VFS_FEATURES", "reserve_exec_cancellation(", "preparation.own_cancellation(SLOT, TOKEN)",
              "OP_VFS_DIRECTORY_REFERENCE", "preparation.cancel()", "*output = preparation.publish()")
    positions = [directory.index(token) for token in tokens]
    assert positions == sorted(positions)
    execution = body_after(source, "auto wki_remote_vfs_clone_exec_file(")
    tokens = ("if (SUPPORT != 0) return SUPPORT", "PinPreparation preparation(PinPurpose::EXECUTABLE)",
              "file->exec_read_view.reset(", "allocate_identity()", "preparation.retain_proxy(STATE)",
              "reserve_exec_cancellation(", "preparation.own_cancellation(CANCEL_SLOT, TOKEN)",
              "OP_VFS_EXEC_VIEW", "cancel_remote_exec_view(CANCEL_SLOT)", "return CANCELED != 0 ? CANCELED : result",
              "*output = preparation.publish()")
    positions = [execution.index(token) for token in tokens]
    assert positions == sorted(positions)
    for client in (directory, execution):
        assert "ProxyOpenRefGuard" not in client and "context.release()" not in client
    guard = body_after(source, "class PinPreparation {")
    assert "record.directory == (purpose_ == PinPurpose::DIRECTORY)" in body_after(guard, "auto detach_locked(")
    fixture = body_after(source, "auto wki_remote_vfs_selftest_pin_preparation_purpose(")
    for token in ("PinPurpose::EXECUTABLE", "PinPurpose::DIRECTORY", "reserve_exec_cancellation(&proxy, 904, true)",
                  "reserve_exec_cancellation(&proxy, 904, false)", "finish_exec_cancellation(SLOT, -EIO, false, false)",
                  "g_pin_private_files_freed.load() != FILES + 4", "published->exec_read_view->content_id != 907", "CLOSED != -ENOTCONN"):
        assert token in fixture, token


def test_retained_pin_close_keeps_outer_file_until_cancellation_retires():
    source = (ROOT / "modules/kern/src/net/wki/remote_vfs.cpp").read_text()
    gate = body_after(source, "auto immutable_pin_close(")
    for token in ("!file.directory_view", "O_CREAT", "O_TRUNC", "pending_len == 0", "FILE_ACCESS_SEARCH_ONLY", "ACCESS == 0"):
        assert token in gate, token
    close = body_after(source, "auto remote_vfs_close(")
    assert close.index("close_immutable_pin(f)") < close.index("MutexGuard io_guard")
    immutable = body_after(source, "auto close_immutable_pin(")
    assert immutable.index("PinCloseOwner owner(file)") < immutable.index("cancel_remote_exec_view(")
    assert "io_lock" not in immutable and "MutexGuard" not in immutable and "flush_write_behind" not in immutable
    owner = body_after(source, "class PinCloseOwner {")
    detach = body_after(owner, "auto take_locked(")
    for token in ("std::exchange(file_, nullptr)", "record.phase == ExecCancellationPhase::OWNED", "record.proxy == proxy_",
                  "record.token == token_", "record.directory == directory_", "record.phase = ExecCancellationPhase::PENDING"):
        assert token in detach, token
    cleanup = body_after(owner, "static void cleanup_for_task(")
    tokens = ("candidate->pid_ != pid", "candidate->take_locked(true)", "s_vfs_lock.unlock()",
              "finish_pin_close_backend(retired.file)", "vfs_finish_file_close(retired.file, retired.metadata, -ECANCELED)")
    positions = [cleanup.index(token) for token in tokens]
    assert positions == sorted(positions)
    assert "cancel_remote_exec_view" not in owner and "vfs_proxy_send_and_wait" not in owner
    global_cleanup = body_after(source, "void wki_remote_vfs_cleanup_for_task(")
    assert global_cleanup.index("finish_or_quiesce_waiter(") < global_cleanup.index("PinCloseOwner::cleanup_for_task(pid)")
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    destroy = body_after(core, "auto vfs_destroy_file(")
    tokens = ("stream_detach_file(f)", "cache_notify_detach_file(f)", "advisory_release_file_owner_locks(f)",
              "CLOSE_MAY_CHANGE_METADATA = f->close_may_change_metadata", "f->fops->vfs_close(f)",
              "finish_file_close(f, CLOSE_MAY_CHANGE_METADATA, close_result)")
    positions = [destroy.index(token) for token in tokens]
    assert positions == sorted(positions)
    final = body_after(core, "auto finish_file_close(")
    tokens = ("metadata_cache_note_exact_path_changed", "vfs_file_clear_path(f)", "f->mount_owner = nullptr",
              "release_mount_from_open_file(MOUNT_OWNER)", "delete f", "return close_result")
    positions = [final.index(token) for token in tokens[:-1]]
    assert positions == sorted(positions) and final.rfind("return close_result;") > final.index("delete f")
    fixture = body_after(source, "auto wki_remote_vfs_selftest_pin_close_lifetime(")
    for token in ("wki_remote_vfs_cleanup_for_task(DIRECTORY_PID)", "wki_remote_vfs_cleanup_for_task(EXEC_PID)",
                  "directory_mount.open_files.load() != 0", "exec_mount.refs.load() != 7", "FINISHED + 2",
                  "vfs_put_file_result(file)", "directory ? -ENOTCONN : -EIO", "PENDING_WRITES_REJECTED"):
        assert token in fixture, token


def test_compute_filesystem_preparation_owns_capture_and_grant_transaction():
    source = (ROOT / "modules/kern/src/net/wki/remote_compute.cpp").read_text()
    owner = body_after(source, "class TaskFilesystemPreparation {")
    constructor = body_after(owner, "TaskFilesystemPreparation(Task*")
    tokens = ("executor_->pending_wki_filesystems = this", "credentials_ = credentials != nullptr ? *credentials : source->credentials()",
              "source->capture_filesystem_context(filesystem_)", "std::memcpy(DEST, SELECTED, LENGTH + 1)",
              "take_filesystem_submission_id(g_next_filesystem_submission)")
    positions = [constructor.index(token) for token in tokens]
    assert positions == sorted(positions)
    for token in ("filesystem_ = *supplied", "filesystem_.cwd_anchor = {}", "LENGTH >= UINT16_MAX", "status_ = -ENOMEM"):
        assert token in constructor, token
    counter = body_after(source, "auto take_filesystem_submission_id(")
    assert "value == UINT64_MAX ? 0 : value + 1" in counter and "while (value != 0)" in counter
    staging = body_after(owner, "auto adopt_staging(")
    for token in ("wki_vfs_directory_grant_valid(grant)", "grant.submission_id != transaction_", "grant.target_node != target_",
                  "grant.target_boot_epoch != target_boot_", "std::exchange(file, nullptr)"):
        assert token in staging, token
    cancel = body_after(owner, "void cancel(")
    tokens = ("std::exchange(staging_.at(index), nullptr)", "vfs_put_file(file)", "filesystem_.cwd_anchor = {}",
              "filesystem_.root_anchor = {}", "heap_cwd_.reset()", "executor_->pending_wki_filesystems = next_")
    positions = [cancel.index(token) for token in tokens]
    assert positions == sorted(positions) and "owns_ && index < staging_.size()" in cancel
    assert "s_compute_lock" not in owner
    cleanup = body_after(source, "void wki_remote_compute_cleanup_for_task(")
    assert cleanup.index("wki_remote_vfs_cleanup_for_task(exiting_task->pid)") < cleanup.index("pending_wki_filesystems->cancel()")
    for marker in ("auto wki_task_submit_inline(", "auto wki_task_submit_vfs_ref("):
        submit = body_after(source, marker)
        assert submit.index("TaskFilesystemPreparation filesystem_preparation(") < submit.index("capture_submit_filesystem(")
        assert "auto const SUBMIT_CREDENTIALS = filesystem_preparation.credentials()" in submit
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    release = body_after(core, "DirectoryAnchorRef::~DirectoryAnchorRef()")
    tokens = ("refs.fetch_sub", "std::exchange(anchor_->remote, nullptr)", "anchor_->mount = nullptr", "delete anchor_", "vfs_put_file(REMOTE)")
    positions = [release.index(token) for token in tokens]
    assert positions == sorted(positions)
    assert "vfs_put_file" not in body_after(core, "~DirectoryAnchor()")
    fixture = body_after(source, "auto wki_remote_compute_selftest_filesystem_preparation(")
    for token in ("boundary{UINT64_MAX - 1}", "TaskFilesystemPreparation other", "ORIGINAL.data()", "wrong.submission_id",
                  "wrong.target_boot_epoch", "state->closed == 3", "wki_remote_compute_cleanup_for_task(&unrelated)"):
        assert token in fixture, token


def test_compute_filesystem_wire_keeps_identity_layout_and_admitted_binding():
    source = (ROOT / "modules/kern/src/net/wki/remote_compute.cpp").read_text()
    wire = (ROOT / "modules/kern/src/net/wki/wire.hpp").read_text()
    assert "static_assert(sizeof(TaskFilesystemContext) == 112)" in wire
    assert "static_assert(sizeof(TaskSubmitPayload) == 20" in wire
    shape = body_after(wire, "constexpr auto wki_task_filesystem_valid(")
    for token in ("context.submission_id == 0", "context.reserved != 0", "grant.submission_id == context.submission_id",
                  "wki_vfs_directory_grant_equal(grant, VfsDirectoryGrant{})", "context.root.source_boot_epoch == context.cwd.source_boot_epoch"):
        assert token in shape, token
    binding = body_after(wire, "constexpr auto wki_task_filesystem_matches_admission(")
    for token in ("grant.source_node == source", "grant.source_boot_epoch == source_boot", "grant.target_node == target",
                  "grant.target_boot_epoch == target_boot"):
        assert token in binding, token
    assert "WKI_TASK_FILESYSTEM_IDENTITY_SIZE = WKI_TASK_ROUTED_IDENTITY_SIZE + sizeof(TaskFilesystemContext)" in source
    build = body_after(source, "auto build_submit_context_info(")
    assert "!wki_task_filesystem_valid(*filesystem)" in build and "WKI_TASK_FILESYSTEM_IDENTITY_SIZE" in build
    copy = body_after(source, "void copy_submit_identity_data(")
    assert copy.index("dest + WKI_TASK_FULL_IDENTITY_SIZE") < copy.index("dest + WKI_TASK_ROUTED_IDENTITY_SIZE")
    prepare = body_after(source, "auto prepare_submitted_resources(")
    assert "!valid_submit_filesystem_identity(*submit)" in prepare
    assert "has_routed_task_identity(submit->identity_len)" in prepare
    for token in ("wki_task_filesystem_valid(filesystem_candidate)", "filesystem_output == nullptr", "TaskRejectReason::RESOURCE_UNSUPPORTED",
                  "submitted_identity.root != ker::mod::sched::task::Task::PathBuffer", "submit_context[submit->args_len - 1] != 0"):
        assert token in prepare, token
    assert prepare.index("resource_contract::restore(") < prepare.index("*filesystem_output = filesystem_candidate")
    for marker in ("auto wki_task_submit_inline(", "auto wki_task_submit_vfs_ref("):
        submit = body_after(source, marker)
        assert "FILESYSTEM = filesystem_preparation.wire_context()" in submit
        assert "FILESYSTEM != nullptr ? WKI_TASK_SUBMIT_FILESYSTEM : 0" in submit
        assert "ROUTED_HOME, FILESYSTEM)" in " ".join(submit.split())
    receiver = source[source.index("bool const FILESYSTEM = (submit->reserved & WKI_TASK_SUBMIT_FILESYSTEM)"):]
    assert receiver.index("session.identity.boot_epoch") < receiver.index("wki_ipc_validate_task_fds(")
    assert "submitted_filesystem.flags != WKI_TASK_FILESYSTEM_CWD_GRANT" in receiver
    assert "submitted_filesystem.cwd.owner_node != g_wki.my_node_id" in receiver
    assert "WKI_FILESYSTEM_CONTEXT_RESOURCE_ID" not in (ROOT / "modules/kern/src/net/wki/remotable.cpp").read_text()
    fixture = body_after(source, "auto wki_remote_compute_selftest_filesystem_wire(")
    for token in ("TaskDeliveryMode::INLINE", "TaskDeliveryMode::VFS_REF", "TaskDeliveryMode::RESOURCE_REF", "missing <= sizeof(TaskFilesystemContext)",
                  "decoded_credentials != credentials", "std::memcmp(&decoded, &BEFORE", "wki_task_filesystem_matches_admission"):
        assert token in fixture, token


def test_return_owner_cwd_connects_grant_to_loader_and_task():
    source = (ROOT / "modules/kern/src/net/wki/remote_compute.cpp").read_text()
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    send = body_after(source, "auto prepare_return_owner_cwd(")
    for token in ("vfs_captured_remote_cwd(preparation.filesystem())", "wki_remote_vfs_directory_owner(directory) != target",
                  "WKI_RETURN_OWNER_CWD_RESOURCE_ID", "admitted.identity = capture_compute_peer_identity(target)",
                  "wki_remote_vfs_issue_directory_grant(directory, preparation.credentials()", "preparation.adopt_staging(1, staging, grant)",
                  "!filesystem_submit_admission_current(target, admitted)", "preparation.finalize_wire(WKI_TASK_FILESYSTEM_CWD_GRANT |"):
        assert token in send, token
    assert send.index("wki_remote_vfs_issue_directory_grant(") < send.index("preparation.adopt_staging(") < send.index("preparation.finalize_wire(")
    for marker in ("auto wki_task_submit_inline(", "auto wki_task_submit_vfs_ref("):
        submit = body_after(source, marker)
        assert submit.index("prepare_return_owner_cwd(") < submit.index("capture_submit_filesystem(") < submit.index("build_submit_context_info(")
        assert submit.index("!filesystem_submit_admission_current(") < submit.index("allocate_submitted_task_id_locked(")
    receiver = body_after(source, "void handle_task_submit_work(")
    tokens = ("prepare_submitted_identity(", "wki_remote_vfs_adopt_directory_grant_native(", "vfs_prepare_global_cwd(",
              "!admission_current()", "ScopedSubmitVfsIdentity submit_vfs_identity(", "load_elf_from_vfs_path(",
              "submit_vfs_identity.install_filesystem_on(*new_task)")
    positions = [receiver.index(token) for token in tokens]
    assert positions == sorted(positions)
    assert "if (!FILESYSTEM) {" in receiver
    swap = body_after(source, "auto swap_task_filesystem(")
    for token in ("task.root_anchor.swap(context.root_anchor)", "task.cwd_anchor.swap(context.cwd_anchor)",
                  "std::swap(task.root, context.root)", "std::swap(task.cwd, context.cwd)"):
        assert swap.index("path_metadata_lock.lock_irqsave()") < swap.index(token) < swap.index("path_metadata_lock.unlock_irqrestore(")
    scoped = body_after(source, "struct ScopedSubmitVfsIdentity")
    assert "filesystem_owner = received_owner" in scoped
    assert receiver.index("received_owner.emplace(current_task, nullptr, nullptr)") < receiver.index("wki_remote_vfs_adopt_directory_grant_native(")
    assert "received_owner->received_filesystem()" in receiver
    cancellation = body_after(body_after(source, "class TaskFilesystemPreparation"), "void cancel()")
    assert "received_view_ = {}" in cancellation and "received_.cwd_anchor = {}" in cancellation
    restore = body_after(scoped, "~ScopedSubmitVfsIdentity()")
    assert "filesystem_owner->filesystem()" in restore and "swap_task_filesystem(*task, restored)" in restore
    borrowed = body_after(core, "auto vfs_captured_remote_cwd(")
    assert "directory_anchor_matches" in borrowed and "filesystem.cwd_anchor.get()->remote" in borrowed
    prepared = body_after(core, "auto vfs_prepare_global_cwd(")
    assert "admit_directory_view" in prepared and "make_directory_anchor_from_pin(retained.directory()" in prepared
    assert "cwd.view = retained" in prepared and "directory_anchor_path(cwd" in prepared
    assert "candidate.root_anchor" not in prepared
    adverts = (ROOT / "modules/kern/src/net/wki/remotable.cpp").read_text()
    assert "send_return_owner_cwd_advert(peer.node_id)" in adverts and "WKI_FILESYSTEM_CONTEXT_RESOURCE_ID" not in adverts


def test_unsupported_root_cannot_hide_retained_remote_cwd():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    borrowed = body_after(core, "auto vfs_captured_remote_cwd(")
    assert "filesystem.root_anchor" not in borrowed and 'std::strcmp(filesystem.root.data(), "/")' not in borrowed
    source = (ROOT / "modules/kern/src/net/wki/remote_compute.cpp").read_text()
    admission = body_after(source, "auto prepare_return_owner_cwd(")
    denial = "if (filesystem.root_anchor) return -ENOTSUP;"
    assert admission.index("if (directory == nullptr) return ker::vfs::vfs_validate_text_filesystem_submission(") < admission.index(denial)
    assert admission.index(denial) < admission.index("wki_remote_vfs_directory_owner(") < admission.index("PeerLifecycleLease lifecycle;")
    for marker in ("auto wki_task_submit_inline(", "auto wki_task_submit_vfs_ref("):
        submit = body_after(source, marker)
        assert submit.index("prepare_return_owner_cwd(") < submit.index("if (FILESYSTEM_PREPARATION != 0)") < submit.index("capture_submit_filesystem(")


def test_text_submission_cannot_discard_native_cwd_scope():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    validate = body_after(core, "auto vfs_validate_text_filesystem_submission(")
    assert "directory_anchor_matches(cwd, filesystem.root.data(), filesystem.cwd.data())" in validate
    assert validate.index("return -ESTALE;") < validate.index("return -ENOTSUP;") < validate.index("return 0;")
    for token in ("filesystem.root_anchor", "cwd.get()->view", "cwd.get()->remote != nullptr"):
        assert token in validate
    for forbidden in ("get_current_task", "vfs_acquire_lookup", "vfs_put_file", "MutexGuard", "new "):
        assert forbidden not in validate
    source = (ROOT / "modules/kern/src/net/wki/remote_compute.cpp").read_text()
    installed = body_after(source, "auto wki_remote_compute_selftest_installed_cwd(")
    assert "wki_remote_compute_selftest_cwd_submission_admission(invalid, -ENOTSUP, credentials)" in installed
    assert "wki_remote_compute_selftest_cwd_submission_admission(stale, -ESTALE, credentials)" in installed
    admission = body_after(source, "auto prepare_return_owner_cwd(")
    assert admission.index("vfs_validate_text_filesystem_submission(") < admission.index("preparation.bind_target(")


def test_independent_filesystem_roots_intersect_identity_and_policy():
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    prepare = body_after(core, "auto vfs_prepare_filesystem_context(")
    assert prepare.index("auto retained_root = process_root") < prepare.index("MutexGuard publication_guard")
    for token in ("previous = root_view.root()", "previous = retained.root()", "boundary = retained.root()", "boundary = root_view.root()"):
        assert token in prepare
    assert prepare.index("admit_directory_view(previous, boundary, retained.directory()") < prepare.index("DirectoryView(boundary")
    assert "retained.policy().restricted_by(root_view.policy())" in prepare
    assert "root.view = root_view" in prepare and "cwd.view = cwd_view" in prepare
    assert "make_directory_anchor_from_pin(root_view.root()" in prepare
    assert "return vfs_prepare_filesystem_context(credentials, source, source, output);" in core


def test_rooted_return_owner_cwd_preserves_declared_root_and_ownership():
    source = (ROOT / "modules/kern/src/net/wki/remote_compute.cpp").read_text()
    send = body_after(source, "auto prepare_return_owner_cwd(")
    assert "if (wki_remote_vfs_directory_owner(directory) != target) return -ENOTSUP;" in send
    assert "ROOT_PATH ? WKI_ROOTED_RETURN_OWNER_CWD_RESOURCE_ID : WKI_RETURN_OWNER_CWD_RESOURCE_ID" in send
    assert "wki_task_legacy_root_path_valid(filesystem.root, false)" in send
    current = body_after(source, "auto filesystem_submit_admission_current(")
    assert "ResourceType::COMPUTE, admitted.resource_id" in current
    prepare = body_after(source, "auto prepare_received_root_path_cwd(")
    tokens = ("wki_task_legacy_root_path_valid(root)", "vfs_prepare_root_directory_view_as(", "vfs_prepare_filesystem_context(",
              "owner.received_prepared().filesystem().root != root", "owner.received_filesystem() =", "owner.received_filesystem().root_anchor = {}")
    positions = [prepare.index(token) for token in tokens]
    assert positions == sorted(positions)
    receiver = body_after(source, "void handle_task_submit_work(")
    assert receiver.index("prepare_submitted_identity(") < receiver.index("prepare_received_root_path_cwd(") < receiver.index("ScopedSubmitVfsIdentity submit_vfs_identity(")
    cancel = body_after(body_after(source, "class TaskFilesystemPreparation"), "void cancel()")
    assert cancel.index("received_prepared_ = {}") < cancel.index("owns_ = false")
    assert cancel.index("received_root_view_ = {}") < cancel.index("owns_ = false")
    scoped = body_after(source, "struct ScopedSubmitVfsIdentity")
    assert "received.root != identity->root" in scoped
    assert "wki_task_legacy_root_path_valid(received.root)" in scoped
    adverts = (ROOT / "modules/kern/src/net/wki/remotable.cpp").read_text()
    assert "send_rooted_return_owner_cwd_advert(peer.node_id)" in adverts
    assert "WKI_ROOTED_RETURN_OWNER_CWD_RESOURCE_ID" in adverts
    assert "WKI_FILESYSTEM_CONTEXT_RESOURCE_ID" not in adverts


def test_pivot_native_covers_publish_with_paths_and_retire_after_unlock():
    mount = (ROOT / "modules/kern/src/vfs/mount.cpp").read_text()
    core = (ROOT / "modules/kern/src/vfs/core.cpp").read_text()
    pivot = body_after(core, "auto vfs_pivot_root(")
    tokens = ("wki_remote_vfs_prepare_export_rebuild()", "PivotMountReferences references", "MutexGuard publication_guard",
              "remap_mounts_for_pivot(", "t->root.data()", "rebase_wki_mounts_for_new_root(", "g_vfs_namespace_generation.fetch_add")
    positions = [pivot.index(t) for t in tokens]
    assert positions == sorted(positions)
    remap = body_after(mount, "auto remap_mounts_for_pivot(")
    tokens = ("bool target_collision = false", "reference_owner.state_ = cover_state", "pin_pivot_cover(REQUEST_CREDENTIALS",
              "mount_has_active_refs_locked(replacement.mount, cover_state)", "entry.retired = mount->covered_directory",
              "mount->covered_directory = std::exchange(entry.candidate, nullptr)", "bump_mount_generation_locked()", "mp->path = replacement.path")
    positions = [remap.index(t) for t in tokens]
    assert positions == sorted(positions)
    pin = body_after(mount, "auto pin_pivot_cover(")
    assert pin.index("output.parent_hold = retain_mount_point(parent)") < pin.index("tmpfs::tmpfs_acquire_lookup(")
    assert "xfs::xfs_acquire_lookup(" in pin and "false)" in pin
    assert "find_mount_point(" not in pin and "mount_table_generation_snapshot(" not in pin
    busy = body_after(mount, "auto mount_has_active_refs_locked(")
    assert "prepared->entries[index].parent_hold.get() == mount" in busy
    assert "pin->refcount.load(std::memory_order_acquire) == 1" in busy
    assert "mount->refs.load(std::memory_order_acquire) != prepared_refs" in busy
    source = (ROOT / "modules/kern/src/net/wki/remote_compute.cpp").read_text()
    receiver = body_after(source, "void handle_task_submit_work(")
    assert "status == -ENOTSUP ? TaskRejectReason::RESOURCE_UNSUPPORTED" in " ".join(receiver.split())


if __name__ == "__main__":
    test_pivot_native_covers_publish_with_paths_and_retire_after_unlock()
    test_rooted_return_owner_cwd_preserves_declared_root_and_ownership()
    test_independent_filesystem_roots_intersect_identity_and_policy()
    test_unsupported_root_cannot_hide_retained_remote_cwd()
    test_text_submission_cannot_discard_native_cwd_scope()
    test_return_owner_cwd_connects_grant_to_loader_and_task()
    test_compute_filesystem_wire_keeps_identity_layout_and_admitted_binding()
    test_compute_filesystem_preparation_owns_capture_and_grant_transaction()
    test_retained_pin_close_keeps_outer_file_until_cancellation_retires()
    test_all_retained_pin_constructors_own_preparation_across_waits()
    test_grant_rpc_preparation_survives_abandoned_executor_waits()
    test_native_directory_grants_publish_exact_authority_and_release_after_unlock()
    test_compute_queues_retain_one_authenticated_peer_identity()
    test_directory_transfer_grants_bind_snapshot_lifetime_and_deferred_admission()
    test_procfs_mount_reads_capture_current_root_without_shared_cache()
    test_procfs_directory_text_uses_exact_inode_and_process_root()
    test_statvfs_admits_retained_caller_lookup_and_stages_outputs()
    test_scoped_native_walk_proves_selected_objects_and_borrows_open_membership()
    test_cwd_recovery_retains_proof_outside_publication_and_rejects_before_capture()
    test_typed_anchor_path_validates_identity_and_requires_outer_cross_mount_proof()
    test_native_component_crossing_selects_covered_objects_and_owns_proof()
    test_lookup_membership_cookie_is_owned_and_consumer_access_is_read_only()
    test_mount_membership_reuses_owned_edges_without_caching_authorization()
    test_complete_filesystem_context_preserves_root_ownership_and_refuses_text_transfer()
    test_filesystem_preparation_owns_both_anchors_without_publishing_task_state()
    test_cross_mount_context_admission_owns_proof_and_preserves_lookup_gate()
    test_procfs_directory_links_capture_one_object_before_text_fallback()
    test_procfs_view_origin_is_owned_and_revalidated_at_native_commit()
    test_directory_files_and_cwd_publish_and_retain_complete_owned_views()
    test_shared_open_stages_direct_file_and_serialized_view_before_publication()
    test_directory_identity_admission_does_not_bypass_current_search_or_grant_data()
    test_directory_view_rebase_owns_detached_pin_and_readmits_after_unlock()
    test_tmpfs_directory_authority_changes_invalidate_prior_component_search()
    test_xfs_directory_authority_invalidates_whole_path_across_mounts()
    test_directory_view_lookup_retains_boundary_and_checks_each_consumer()
    test_serialized_lookup_releases_captured_references_after_publication()
    test_owned_directory_views_narrow_policy_and_publish_outside_locks()
    test_retained_directory_boundary_uses_native_objects_and_stages_outputs()
    test_native_mount_membership_uses_covered_identity_and_current_attachment()
    test_lookup_context_includes_nonreused_cwd_anchor_identity()
    test_lookup_context_includes_canonical_root_anchor_identity()
    test_absolute_task_lookup_uses_owned_root_and_preserves_search_semantics()
    test_relative_owned_root_lookup_intersects_native_views_and_policy()
    test_xfs_lookup_mutations_consume_explicit_readonly_policy()
    test_open_and_exec_replacement_preserve_admitted_view_policy()
    test_lookup_consumers_use_selected_mount_policy()
    test_sync_preflights_all_ranges_and_serializes_mapping_identity()
    test_file_mapping_uses_one_description_and_preflights_policy()
    test_writeback_and_replay_use_retained_file_policy()
    test_retained_file_policy_denies_before_data_and_metadata_effects()

    test_gate_requires_every_domain_even_when_remaining_rows_are_enforced()
    test_remote_realpath_delegates_complete_remainder_and_stages_output()
    test_native_realpath_uses_retained_lookup_before_legacy_caches()
    test_remote_open_at_reserves_before_effect_and_never_uses_path_authority()
    test_remote_directory_paths_keep_exact_descriptor_and_snapshot()
    test_remote_alias_handoff_selects_complete_raw_export_and_preserves_grants()
    test_owner_directory_grant_uses_retained_inode_and_admitted_snapshot()
    test_initial_mapped_routes_do_not_fall_back_to_normalized_text()
    test_truncate_checks_retained_write_grant_before_effect()
    test_mutation_batches_map_one_snapshot_before_any_item()
    test_mapped_components_require_negotiation_and_preserve_raw_ancestors()
    test_remote_readlink_uses_mapped_snapshot_without_cache_or_fallback()
    test_routed_lookup_preserves_admitted_snapshot_across_resolution()
    test_foreign_local_routes_keep_original_ancestors_and_checked_handoff()
    test_local_submitter_rules_do_not_bypass_native_ancestor_walk()
    test_local_readlink_checks_retained_directory_and_devfs_before_fallback()
    test_explicit_owner_readlink_stages_bytes_under_one_snapshot()
    test_remote_path_stat_preserves_caller_identity_and_no_legacy_fallback()
    test_remote_fstat_uses_retained_descriptor_and_staged_output()
    test_owner_confinement_is_enforced_during_component_traversal()
    test_explicit_owner_stat_stages_metadata_under_one_snapshot()
    test_executable_cache_candidates_require_retained_source_content()
    test_remote_elf_source_requires_mapped_read_before_cache_or_construction()
    test_remote_explicit_read_carries_admitted_policy_and_never_downgrades()
    test_remote_exec_cannot_erase_restrictions()
    test_process_restriction_abi_and_publication()
    test_mount_target_uses_admission_snapshot_before_preparation()
    test_mount_admission_precedes_side_effects()
    test_lexer_ignores_comments_and_literals_but_detects_root_spellings()
    test_gate_rejects_equivalent_integral_zero_checks()
    test_gate_rejects_new_duplicate_and_removed_exceptions()
    test_real_tree_matches_manifest()
    test_duplicate_keys_are_rejected()
    test_control_authorization_precedes_side_effects()
    test_ptrace_and_dumpability_use_shared_policy()
    test_ptrace_register_validation_precedes_mutation()
    test_shm_authorization_precedes_pins_and_output()
    test_shm_write_limit_survives_mapping_lifetime()
    test_tmpfs_metadata_authorizes_locked_object_before_commit()
    test_xfs_metadata_checks_before_transaction_and_preserves_lookup_identity()
    test_stat_authority_tuples_are_copied_under_writer_locks()
    test_devfs_metadata_checks_retained_identity_before_publication()
    test_fat_metadata_authorizes_fixed_projection()
    test_timestamp_policy_precedes_locked_mutation_and_transaction()
    test_remote_directory_references_map_admission_and_keep_cleanup_ownership()
    test_owner_directory_pins_retain_complete_views_and_deny_without_refresh()
    test_retained_directory_name_recovery_preserves_confinement()
    test_directory_pins_use_search_authority_without_data_grants()
    test_xattr_access_and_visibility_use_explicit_snapshot()
    test_tmpfs_open_authorization_precedes_publication_and_truncation()
    test_xfs_open_authorization_precedes_creation_and_file_publication()
    test_fat_open_denials_precede_creation_and_propagate_errno()
    test_devfs_open_checks_before_callbacks_and_readlink_has_no_open()
    test_procfs_global_policy_covers_types_and_precedes_content_or_controls()
    test_procfs_metadata_retains_owner_and_stages_before_publication()
    test_procfs_open_retains_identity_and_publishes_only_after_validation()
    test_procfs_final_readlink_retains_request_identity_and_stages_before_publication()
    test_procfs_root_consumers_use_canonical_value_snapshot()
    test_procfs_fd_access_authorizes_and_revalidates_before_publication()
    test_private_procfs_pins_identity_and_revalidates_cache_links_and_enumeration()
    test_resource_owner_uses_immutable_credential_subject_before_policy()
    test_procfs_limits_keep_admitted_caller_and_retained_subject()
    test_journal_device_open_consumes_devfs_admission_snapshot()
    test_device_open_uses_explicit_task_authority()
    test_public_procfs_uses_pinned_memory_and_bounded_names_before_cache_publication()
    test_wki_procfs_identity_copies_pin_tasks_and_release_locks_before_formatting()
    test_removal_authorization_precedes_namespace_and_disk_effects()
    test_directory_paths_are_recovered_from_pinned_identity_before_dispatch()
    test_explicit_regular_read_keeps_request_authority_separate_from_process_credentials()
    test_replaced_exec_worker_cannot_publish_credentials()
    test_exec_nosuid_uses_retained_mount_policy()
    test_rename_pair_keeps_explicit_admitted_identity()
    test_linkat_pair_keeps_one_snapshot_through_both_lookups_and_retries()
    test_ptrace_exception_stop_publishes_before_nonreturning_handoff()
    print("credential policy audit tests passed")

    test_owned_root_getcwd_uses_native_metadata_and_intersects_views()

    test_owned_root_visible_consumers_pair_capture_and_stage_publication()

    test_procfs_owned_root_and_scoped_fd_metadata_preserve_subject_boundary()

    test_procfs_plain_directory_pins_exact_native_file_beneath_owned_root()
