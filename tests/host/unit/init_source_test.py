#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
INIT_CPP = ROOT / "modules" / "init" / "src" / "init.cpp"
SERVICES_CPP = ROOT / "modules" / "init" / "src" / "services.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|void|int)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
        source,
        flags=re.DOTALL,
    )
    if match is None:
        fail(f"missing function {name}")

    depth = 1
    pos = match.end()
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated function {name}")
    return source[match.end() : pos - 1]


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def forbid_tokens(source: str, tokens: list[str], context: str) -> None:
    present = [token for token in tokens if token in source]
    if present:
        fail(f"{context}: forbidden {', '.join(present)}")


def require_order(source: str, snippets: list[str], context: str) -> None:
    cursor = 0
    for snippet in snippets:
        found = source.find(snippet, cursor)
        if found < 0:
            fail(f"{context}: missing ordered snippet {snippet}")
        cursor = found + len(snippet)


def test_manifest_load_and_failed_mode_are_bounded() -> None:
    source = SERVICES_CPP.read_text()
    require_tokens(
        source,
        [
            'SERVICE_MANIFEST_PATH[] = "/etc/wos-services.conf"',
            "std::array<char, MAX_MANIFEST_BYTES> manifest_bytes",
            "while (size < runtime.manifest_bytes.size())",
            "read_extra = ::read(FD, &extra, 1)",
            "parse_service_manifest",
            "validate_service_manifest",
            "resolve_enablement",
            "EnablementKind::PATH_EXISTS",
            "EnablementKind::PATH_MISSING",
            "runtime.failed = true",
            "publish_status(runtime.now_ms)",
        ],
        "bounded manifest load and failed-supervisor status",
    )

    start_body = function_body(source, "start_service_supervisor")
    require_order(
        start_body,
        [
            "restore_init_wki_target()",
            "read_manifest(manifest_size)",
            "parse_service_manifest",
            "validate_service_manifest",
            "initialize_supervisor",
            "advance_model(runtime.now_ms)",
            "publish_status(runtime.now_ms)",
        ],
        "manifest validation precedes service actions and status publication",
    )


def test_spawn_has_exec_report_pgid_wki_and_fixed_vectors() -> None:
    source = SERVICES_CPP.read_text()
    spawn_body = function_body(source, "spawn_service")
    require_tokens(
        source,
        [
            "INIT_WKI_TARGET_FLAGS = ker::process::WKI_TARGET_FLAG_LOCAL | ker::process::WKI_TARGET_FLAG_NOINHERIT",
            "SERVICE_WKI_TARGET_FLAGS = ker::process::WKI_TARGET_FLAG_LOCAL",
            "std::array<const char*, MAX_ARGUMENTS + 1> arguments",
            "std::array<const char*, MAX_RUNTIME_ENVIRONMENT> environment",
            "manifest_overrides_environment",
            "F_DUPFD_CLOEXEC",
            "move_above_standard_io(pipefd.at(PIPE_READ))",
            "move_above_standard_io(pipefd.at(PIPE_WRITE))",
            "set_nonblocking(pipefd.at(PIPE_READ))",
            "set_close_on_exec(pipefd.at(PIPE_WRITE))",
            "ExecFailureReport",
            "report_child_failure",
            "ChildFailureStage::EXEC",
        ],
        "fixed spawn and CLOEXEC report-pipe surface",
    )
    require_tokens(
        spawn_body,
        [
            "ker::process::setwkitarget(nullptr, 0, SERVICE_WKI_TARGET_FLAGS)",
            "ker::process::fork()",
            "restore_init_wki_target()",
            "ker::process::setpgid(0, 0)",
            "ker::process::setpgid(PID, PID)",
            "ker::process::getpgid(PID)",
            "OBSERVED_PGID != PID",
            "retain_cleanup_child()",
            "ker::process::setpriority(PRIO_PROCESS, 0, spec.priority)",
            "ker::process::execve(spec.executable.c_str(), arguments.data(), environment.data())",
        ],
        "generation-owned child setup",
    )
    require_order(
        spawn_body,
        [
            "ker::process::setwkitarget(nullptr, 0, SERVICE_WKI_TARGET_FLAGS)",
            "ker::process::fork()",
            "restore_init_wki_target()",
        ],
        "PID 1 restores LOCAL|NOINHERIT immediately after fork",
    )


def test_tick_owns_reaping_exec_order_drains_probes_and_control() -> None:
    source = SERVICES_CPP.read_text()
    tick_body = function_body(source, "service_supervisor_tick")
    require_tokens(
        source,
        [
            "MAX_REAPS_PER_TICK",
            "ker::process::waitpid(-1, &status, WNOHANG, nullptr)",
            "reaped unknown/adopted child",
            "process_exec_report",
            "OUTPUT_READ_BYTES_PER_TICK",
            "drain_service_output",
            "ker::logging::logEx(name.c_str()",
            "leader_reaped",
            "service_group_gone(slot)",
            "SupervisorEventKind::QUIESCED",
            "SupervisorResult::STALE_GENERATION",
            "ignore_stale_event",
            "MAX_STALE_EVENT_LOGS",
            "ker::process::kill(-slot.pgid, signal)",
            "network_probe_begin",
            "network_probe_poll",
            "network_probe_dump_diagnostics",
            "index <= control_abi::MAILBOX_CAPACITY",
            "ker::process::init_control_receive",
            "ker::process::init_status_publish",
            "snapshot.sequence = 0",
            "control_abi::MAX_TRANSITION_HISTORY",
        ],
        "central tick ownership surface",
    )
    if source.count("ker::process::waitpid(-1, &status, WNOHANG, nullptr)") != 1:
        fail("services runtime must contain exactly one central waitpid(-1, WNOHANG) call")
    require_order(
        tick_body,
        [
            "process_exec_report(service, NOW_MS)",
            "drain_service_output(service)",
            "poll_network_probes(NOW_MS)",
            "reap_children(NOW_MS)",
            "drain_control_mailbox(NOW_MS)",
            "advance_model(NOW_MS)",
            "publish_status(NOW_MS)",
        ],
        "exec outcome precedes reap and every tick publishes status",
    )
    forbid_tokens(source, ["DRAIN_PID", "spawn_with_journal_stdio"], "PID 1 must own output drains without drain children")


def test_root_init_drives_common_supervisor_without_hardcoded_services() -> None:
    source = INIT_CPP.read_text()
    require_tokens(
        source,
        [
            "start_service_supervisor()",
            "service_supervisor_tick()",
            "SERVICE_SUPERVISOR_TICK_MS * 1000L * 1000L",
            "service supervisor initialization failed; PID 1 remains alive for reaping and shutdown",
        ],
        "root init supervisor loop",
    )
    forbid_tokens(
        source,
        ["start_journald()", "start_network()", "start_httpd()", "start_dropbear()", "start_testd()"],
        "root init no longer hardcodes service launch order",
    )


def test_pivot_root_retries_transient_busy_mount_refs() -> None:
    source = INIT_CPP.read_text()
    require_tokens(
        source,
        [
            "#include <cerrno>",
            "PIVOT_ROOT_MAX_ATTEMPTS",
            "PIVOT_ROOT_RETRY_MS",
            "pivot_root_with_busy_retry",
            "ret = ker::abi::vfs::pivot_root_vfs(ROOTFS_MOUNTPOINT, OLD_ROOT_MOUNTPOINT)",
            "if (ret != -EBUSY)",
            'init_log::warn("init[%llu]: pivot_root busy (attempt %d/%d), retrying"',
            "sleep_ms(PIVOT_ROOT_RETRY_MS)",
            "int const PIVOT_RET = pivot_root_with_busy_retry(CPUNO)",
            "pivot_root failed (ret=%d), continuing with initramfs root",
        ],
        "init pivot_root transient busy retry",
    )


def main() -> None:
    test_pivot_root_retries_transient_busy_mount_refs()
    test_manifest_load_and_failed_mode_are_bounded()
    test_spawn_has_exec_report_pgid_wki_and_fixed_vectors()
    test_tick_owns_reaping_exec_order_drains_probes_and_control()
    test_root_init_drives_common_supervisor_without_hardcoded_services()
    print("init declarative supervisor source invariants hold")


if __name__ == "__main__":
    main()
