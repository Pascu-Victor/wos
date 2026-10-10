#!/usr/bin/env python3
"""Focused ptrace source contracts; native KTEST supplies execution evidence."""
from credential_policy_audit_test import (
    ROOT, body_after, test_ptrace_and_dumpability_use_shared_policy,
    test_ptrace_register_validation_precedes_mutation,
)


def main():
    test_ptrace_and_dumpability_use_shared_policy()
    test_ptrace_register_validation_precedes_mutation()
    source = (ROOT / "modules/kern/src/platform/debug/ptrace.cpp").read_text()
    dispatch = body_after(source, "auto ptrace_as(")
    assert dispatch.index("operation.admit()") < dispatch.index("get_target(")
    selector = body_after(source, "auto get_target(")
    assert "find_task_by_visible_pid_safe(tracer, pid)" in selector
    assert "find_task_by_pid_safe(" not in selector
    traceme = body_after(dispatch, "if (req == abi::ptrace::request::TRACEME)")
    assert "scope->is_host() ?" in traceme and "acquire_namespace_parent(*tracer)" in traceme
    assert traceme.index("operation.own_target(*parent)") < traceme.index("attach_tracer(")
    effect = body_after(source, "auto begin_effect(")
    assert effect.index("same_pid_namespace(") < effect.index("PublicationGuard") < effect.index("begin_local_signal_admission(")
    assert effect.index("pagemap != owner->pagemap") < effect.index("begin_local_signal_admission(")
    assert "if (LOCAL.remote) return -ENOTSUP" in effect
    remote = body_after(source, "auto remote_info(")
    assert remote.index("operation->private_scope() == nullptr") < remote.index("wki_submitted_task_remote_info(")
    wait = body_after(source, "auto syscall_wait(")
    assert "EffectAdmission" not in wait and "10'000" in wait
    for path, subject in (("syscalls_impl/process/exit.cpp", "current_task"), ("syscalls_impl/multiproc/threadControl.cpp", "task")):
        exit_source = (ROOT / "modules/kern/src" / path).read_text()
        assert exit_source.index("ptrace::cleanup_operations(*" + subject + ")") < exit_source.index("cancel_fd_reservations(*" + subject + ")")
    namespace = (ROOT / "modules/kern/src/platform/sched/pid_namespace.cpp").read_text()
    lease = body_after(namespace, "auto OperationLease::begin(")
    assert lease.index("ADMISSION.open()") < lease.index("operations.fetch_add(")
    cleanup = body_after(source, "void PendingOperation::cancel(")
    assert cleanup.index("buffers_->cancel()") < cleanup.index("finish_effect()") < cleanup.index("target->release()")
    print("Focused ptrace policy, namespace admission, and cleanup source contracts passed")


if __name__ == "__main__":
    main()
