#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
POWERCTL_CPP = ROOT / "modules" / "powerctl" / "src" / "main.cpp"
INIT_SHUTDOWN_CPP = ROOT / "modules" / "init" / "src" / "shutdown.cpp"
INIT_SERVICES_CPP = ROOT / "modules" / "init" / "src" / "services.cpp"
INIT_FSTAB_CPP = ROOT / "modules" / "init" / "src" / "fstab.cpp"
INIT_WRAPPERS_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "init" / "init_wrappers.cpp"
POWER_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "power" / "power.cpp"
POWER_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "power" / "power.hpp"
POWER_ABI = ROOT / "modules" / "kern" / "src" / "abi" / "callnums" / "power.h"
SYS_POWER_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "power" / "sys_power.cpp"
MLIBC_POWER_H = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "include" / "sys" / "power.h"
SCHEDULER_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.hpp"
SCHEDULER_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "scheduler.cpp"
TASK_HPP = ROOT / "modules" / "kern" / "src" / "platform" / "sched" / "task.hpp"
NTP_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "ntp" / "ntp.cpp"
MOUNT_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "mount.cpp"
VFS_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "core.cpp"
ACPI_POWER_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "acpi" / "power.cpp"
ROOTFS_ALIASES = ROOT / "configs" / "rootfs" / "aliases.tsv"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"(?:\b(?:auto|void)|\[\[noreturn\]\]\s+void)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
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


def require_order(source: str, first: str, second: str, context: str) -> None:
    first_index = source.find(first)
    second_index = source.find(second)
    if first_index < 0:
        fail(f"{context}: missing {first}")
    if second_index < 0:
        fail(f"{context}: missing {second}")
    if first_index >= second_index:
        fail(f"{context}: expected {first!r} before {second!r}")


def test_shutdown_command_parses_v1_surface() -> None:
    source = POWERCTL_CPP.read_text()
    require_tokens(
        source,
        [
            'std::strcmp(NAME, "reboot") == 0',
            'std::strcmp(NAME, "halt") == 0',
            'std::strcmp(NAME, "poweroff") == 0',
            'std::strcmp(argv[i], "-r") == 0',
            'std::strcmp(argv[i], "-p") == 0',
            'std::strcmp(argv[i], "-h") == 0',
            'std::strcmp(argv[i], "-f") == 0',
            'std::strcmp(argv[i], "-c") == 0',
            'std::strcmp(when, "now") == 0',
            'return parse_relative_minutes("0", mono_ns, deadline_mono_ns)',
            'return "+0"',
            "canonical_when(when)",
            "when[0] == '+'",
            "std::strchr(when, ':')",
            "CLOCK_MONOTONIC",
            "CLOCK_REALTIME",
            "ker::process::kill(1, SIGHUP)",
            "reboot(reboot_cmd(action))",
        ],
        "shutdown command v1 parsing and dispatch",
    )

    send_body = function_body(source, "send_request")
    require_tokens(
        send_body,
        [
            "REQUEST_TMP_PATH",
            "O_CREAT | O_TRUNC | O_WRONLY",
            '"action=%s\\n"',
            '"deadline_mono_ns=%llu\\n"',
            '"original_when=%s\\n"',
            '"requester_pid=%llu\\n"',
            '"cancel=%d\\n"',
            "ker::abi::vfs::rename(REQUEST_TMP_PATH, REQUEST_PATH)",
            "ker::process::kill(1, SIGHUP)",
        ],
        "shutdown request file writer",
    )


def test_rootfs_aliases_install_power_commands() -> None:
    aliases = ROOTFS_ALIASES.read_text()
    require_tokens(
        aliases,
        [
            "build/modules/powerctl/powerctl\t/usr/sbin/powerctl",
            "/usr/sbin/powerctl\t/sbin/reboot",
            "/usr/sbin/powerctl\t/sbin/poweroff",
            "/usr/sbin/powerctl\t/sbin/halt",
            "/usr/sbin/powerctl\t/sbin/shutdown",
            "/usr/sbin/powerctl\t/usr/bin/reboot",
            "/usr/sbin/powerctl\t/usr/bin/poweroff",
            "/usr/sbin/powerctl\t/usr/bin/halt",
            "/usr/sbin/powerctl\t/usr/bin/shutdown",
        ],
        "rootfs power command aliases",
    )


def test_init_owns_scheduled_shutdown_and_service_sync_order() -> None:
    shutdown_source = INIT_SHUTDOWN_CPP.read_text()
    fstab_source = INIT_FSTAB_CPP.read_text()
    services_source = INIT_SERVICES_CPP.read_text()

    require_tokens(
        fstab_source,
        [
            'REQUIRED_RUN_PATH = "/run"',
            "mount_required_tmpfs_at(CPUNO, REQUIRED_RUN_PATH)",
            "std::strcmp(mountpoint.data(), REQUIRED_RUN_PATH) == 0",
        ],
        "init /run tmpfs mount",
    )
    require_tokens(
        shutdown_source,
        [
            "REQUEST_PATH = \"/run/wos-shutdown.request\"",
            "ker::abi::vfs::unlink(REQUEST_PATH)",
            "case SIGHUP:",
            "case SIGTERM:",
            "case SIGUSR1:",
            "case SIGUSR2:",
            "SIGNAL_WAKE_REQUEST",
            "SIGNAL_REBOOT_NOW",
            "SIGNAL_HALT_NOW",
            "SIGNAL_POWEROFF_NOW",
            "auto consume_signal_action(bool& applied_request) -> ShutdownAction",
            "applied_request = true;",
            "bool applied_request = false;",
            "if (applied_request)",
            "POWER_OP_PREPARE = 2",
            "prepare_kernel_shutdown()",
            "FIVE_MINUTES_NS",
            "ONE_MINUTE_NS",
            'init_log::info("shutdown: scheduled action in <=5 minutes")',
            'init_log::info("shutdown: scheduled action in <=1 minute")',
            'init_log::info("shutdown: deadline reached; starting action=%d"',
        ],
        "init scheduled shutdown signal and deadline handling",
    )
    forbid_tokens(
        shutdown_source,
        [
            'init_log::warn("shutdown: scheduled action in <=5 minutes")',
            'init_log::warn("shutdown: scheduled action in <=1 minute")',
            'init_log::warn("shutdown: deadline reached; starting action=%d"',
            'init_log::warn("shutdown: stopping services")',
            'init_log::warn("shutdown: syncing filesystems before journald stop")',
            'init_log::warn("shutdown: final userspace sync")',
        ],
        "normal shutdown milestones must not be logged as warnings",
    )
    poll_body = function_body(shutdown_source, "shutdown_poll")
    require_order(
        poll_body,
        "if (applied_request)",
        "uint64_t const NOW = monotonic_ns();",
        "new shutdown requests are armed before a later poll evaluates the deadline",
    )

    perform_body = function_body(shutdown_source, "shutdown_perform")
    require_tokens(
        perform_body,
        [
            'init_log::info("shutdown: stopping services")',
            'init_log::info("shutdown: preparing kernel threads")',
            'init_log::info("shutdown: syncing filesystems before journald stop")',
            'init_log::info("shutdown: final userspace sync")',
        ],
        "normal shutdown service/sync milestones use info level",
    )
    require_order(perform_body, "stop_services_for_shutdown()", "prepare_kernel_shutdown()", "services stop before kernel prepare")
    require_order(perform_body, "prepare_kernel_shutdown()", "ker::abi::vfs::sync_vfs()", "kernel prepare before first sync")
    require_order(perform_body, "ker::abi::vfs::sync_vfs()", "stop_journald_for_shutdown()", "journald survives first sync")
    require_order(perform_body, "stop_journald_for_shutdown()", "reboot(CMD)", "journald stopped before finalizer")

    require_tokens(
        services_source,
        [
            "ServiceKind::NETWORK",
            "ServiceKind::NORMAL",
            "ServiceKind::JOURNAL",
            "stop_services_by_kind(ServiceKind::NETWORK)",
            "stop_services_by_kind(ServiceKind::NORMAL)",
            "stop_services_by_kind(ServiceKind::JOURNAL)",
            "wait_for_child_exit(static_cast<int64_t>(service.pid), &status, SERVICE_TERM_TIMEOUT_MS)",
            "wait_for_child_timeout(static_cast<int64_t>(service.pid), &status, SERVICE_KILL_TIMEOUT_MS)",
        ],
        "init service shutdown sequencing",
    )


def test_kernel_finalizer_sequences_teardown_sync_unmount_and_platform_action() -> None:
    source = POWER_CPP.read_text()
    power_hpp = POWER_HPP.read_text()
    power_abi = POWER_ABI.read_text()
    sys_power = SYS_POWER_CPP.read_text()
    mlibc_power = MLIBC_POWER_H.read_text()
    init_wrappers = INIT_WRAPPERS_CPP.read_text()
    run_body = function_body(source, "run_finalizer")
    require_tokens(
        source,
        [
            "compare_exchange_weak(phase, ShutdownPhase::FINALIZING",
            "ShutdownPhase::PREPARING",
            "prepare_shutdown()",
            "quiesce_kernel_threads",
            "sched::request_kernel_threads_shutdown(timeout_us)",
            "quiesce_kernel_threads(KERNEL_THREAD_WAIT_US)",
            "quiesce_kernel_threads(0)",
            "return -EBUSY;",
            "return -EPERM;",
            "RB_ENABLE_CAD",
            "RB_DISABLE_CAD",
            "signal_visible_processes_except(EXCLUDED_PID, EXCLUDED_OWNER_PID, WOS_SIGTERM)",
            "signal_visible_processes_except(EXCLUDED_PID, EXCLUDED_OWNER_PID, WOS_SIGKILL)",
            "clear_current_finalizer_sched_state",
            "current->deferred_task_switch = false;",
            "current->yield_switch = false;",
            "current->wakeup_pending.store(false, std::memory_order_release);",
            "current->set_voluntary_blocked(false);",
            "current->clear_wait_channel();",
            "ker::net::wki::wki_shutdown();",
            "TERM_WAIT_US",
            "KILL_WAIT_US",
            "reboot_via_fadt()",
            "outb(0xcf9, 0x02)",
            "outb(0x64, 0xfe)",
            "poweroff_via_s5()",
            "outw(0x604, 0x2000)",
            'log::info("system shutdown finalizer starting action=%lu"',
        ],
        "kernel shutdown finalizer surface",
    )
    require_tokens(power_hpp, ["PREPARING = 7", "prepare_shutdown()"], "power prepare diagnostic phase is append-only")
    require_tokens(power_hpp, ["selftest_poweroff()"], "KTEST poweroff hook declaration")
    require_tokens(source, ["selftest_poweroff()", 'log::info("selftest complete; powering off")', "platform_poweroff();"], "KTEST poweroff hook")
    require_tokens(power_abi, ["PREPARE = 2"], "power syscall prepare op is append-only")
    require_tokens(sys_power, ["case ker::abi::power::ops::PREPARE:", "ker::mod::power::prepare_shutdown()"], "power syscall prepare dispatch")
    require_tokens(mlibc_power, ["static inline int64_t prepare_shutdown()", "ops::PREPARE"], "mlibc power prepare wrapper")
    forbid_tokens(source, ['log::warn("system shutdown finalizer starting action=%lu"'], "normal finalizer start is not a warning")
    for first, second, context in [
        ("quiesce_kernel_threads(KERNEL_THREAD_WAIT_US);", "teardown_processes();", "kernel threads quiesce before process teardown"),
        ("teardown_processes();", "ker::vfs::vfs_shutdown_sync();", "process teardown before vfs sync"),
        ("teardown_processes();", "ker::net::wki::wki_shutdown();", "process teardown before WKI shutdown"),
        ("ker::net::wki::wki_shutdown();", "ker::vfs::vfs_shutdown_sync();", "WKI shutdown before vfs sync"),
        ("ker::net::wki::wki_shutdown();", "ker::vfs::vfs_shutdown_unmount_all(current_root_path())", "WKI shutdown before root unmount"),
        ("ker::vfs::vfs_shutdown_sync();", "ker::vfs::vfs_shutdown_unmount_all(current_root_path())", "sync before root unmount"),
        ("ker::vfs::vfs_shutdown_unmount_all(current_root_path())", "smt::halt_other_cores();", "root unmount before CPU quiesce"),
        ("smt::halt_other_cores();", "platform_reboot();", "CPU quiesce before platform reboot"),
    ]:
        require_order(run_body, first, second, context)

    sched_init_body = function_body(init_wrappers, "sched_init")
    require_tokens(sched_init_body, ["strcmp(get_kernel_cmdline(), \"--selftest\") == 0", "mod::power::selftest_poweroff();"], "KTEST selftest poweroff call")
    require_order(sched_init_body, "ker::test::run_all();", "mod::power::selftest_poweroff();", "KTEST powers off only after the selftest runner returns")
    require_order(sched_init_body, "mod::power::selftest_poweroff();", "hcf();", "KTEST hcf remains fallback after poweroff")


def test_kernel_thread_shutdown_is_scheduler_owned_not_ntp_specific() -> None:
    scheduler_hpp = SCHEDULER_HPP.read_text()
    scheduler_cpp = SCHEDULER_CPP.read_text()
    task_hpp = TASK_HPP.read_text()
    ntp_source = NTP_CPP.read_text()
    request_body = function_body(scheduler_cpp, "request_kernel_threads_shutdown")
    exit_body = function_body(scheduler_cpp, "maybe_exit_current_kernel_thread_for_shutdown")
    require_tokens(
        task_hpp,
        ["std::atomic<bool> kernel_shutdown_requested{false};"],
        "task-owned kernel shutdown request bit",
    )
    require_tokens(
        scheduler_hpp,
        [
            "struct KernelThreadShutdownResult",
            "maybe_exit_current_kernel_thread_for_shutdown();",
            "auto request_kernel_threads_shutdown(uint64_t timeout_us) -> KernelThreadShutdownResult;",
        ],
        "scheduler kernel-thread shutdown API",
    )
    require_tokens(
        scheduler_cpp,
        [
            "std::atomic<bool> kernel_thread_shutdown_requested{false};",
            "is_kernel_thread_shutdown_target",
            "count_kernel_thread_shutdown_targets",
            "kernel_threads_shutdown_requested()",
        ],
        "scheduler-owned kernel-thread shutdown state",
    )
    require_tokens(
        request_body,
        [
            "kernel_thread_shutdown_requested.store(true, std::memory_order_release);",
            "bool const WAKE_TARGETS = timeout_us != 0;",
            "task->kernel_shutdown_requested.exchange(true, std::memory_order_acq_rel)",
            "task->wakeup_pending.store(true, std::memory_order_release);",
            "wake_kernel_thread_for_shutdown(task);",
            "count_kernel_thread_shutdown_targets(current)",
            "wait_for_kernel_shutdown_progress(current);",
        ],
        "scheduler request/wake/wait kernel-thread shutdown loop",
    )
    require_tokens(
        exit_body,
        [
            "task->type != task::TaskType::DAEMON",
            "task->kernel_shutdown_requested.load(std::memory_order_acquire)",
            "ker::syscall::process::wos_proc_exit(0);",
        ],
        "kernel threads exit through normal task exit",
    )
    require_order(scheduler_hpp, "maybe_exit_current_kernel_thread_for_shutdown();", "if (preempt_count() != 0)", "scheduler block points check shutdown first")
    forbid_tokens(ntp_source, ["power::shutdown_in_progress()", "#include <platform/power/power.hpp>"], "NTP must not carry shutdown-specific checks")


def test_shutdown_unmount_is_exact_deepest_first_and_root_last() -> None:
    mount_source = MOUNT_CPP.read_text()
    vfs_source = VFS_CPP.read_text()
    body = function_body(mount_source, "shutdown_unmount_all_exact")
    require_tokens(
        mount_source,
        [
            "#include <platform/power/power.hpp>",
            "ker::mod::power::shutdown_in_progress()",
            "return -ESHUTDOWN;",
            "sort_shutdown_mounts",
            "mount_should_come_after",
            "is_shutdown_root_mount",
            "mount_path_depth",
            "sync_mount_for_shutdown",
            "wait_for_mount_refs_to_drain_bounded",
            "mp->retiring.store(true, std::memory_order_release)",
            "mounts.remove_at(0)",
            "destroy_mount(mp)",
        ],
        "shutdown mount ordering helpers",
    )
    require_order(body, "sync_mount_for_shutdown(mp)", "wait_for_mount_refs_to_drain_bounded(mp)", "sync before ref drain")
    require_order(body, "wait_for_mount_refs_to_drain_bounded(mp)", "destroy_mount(mp)", "ref drain before destroy")
    if "resolve_mount_path" in body:
        fail("shutdown_unmount_all_exact() must consume exact mount paths, not current-task path resolution")
    require_tokens(
        vfs_source,
        [
            "#include <platform/power/power.hpp>",
            "if (ker::mod::power::shutdown_in_progress())",
            "auto vfs_shutdown_sync() -> int",
            "auto vfs_shutdown_unmount_all(const char* root_path) -> int",
            "return shutdown_unmount_all_exact(root_path);",
        ],
        "vfs shutdown wrappers",
    )
    vfs_mount_body = function_body(vfs_source, "vfs_mount")
    require_order(
        vfs_mount_body,
        "if (ker::mod::power::shutdown_in_progress())",
        "if (source != nullptr)",
        "vfs_mount must close admission before remote WKI mount handling",
    )


def test_acpi_power_validates_fadt_reset_pm1_and_s5() -> None:
    source = ACPI_POWER_CPP.read_text()
    require_tokens(
        source,
        [
            'parse_acpi_tables("FACP")',
            "FADT_RESET_REG_SUP",
            "GenericAddress",
            "GAS_SYSTEM_IO",
            "GAS_SYSTEM_MEMORY",
            "gas_read(pm1a, current)",
            "gas_write(pm1a, VALUE)",
            "gas_read(pm1b, current)",
            "gas_write(pm1b, VALUE)",
            '"_S5_"',
            "AML_PACKAGE_OP",
            "ACPI_PM1_SLP_EN",
            "ACPI_PM1_SLP_TYP_SHIFT",
            "reset_reg",
            "reset_value",
        ],
        "ACPI reset/poweroff parser",
    )


def main() -> None:
    test_shutdown_command_parses_v1_surface()
    test_rootfs_aliases_install_power_commands()
    test_init_owns_scheduled_shutdown_and_service_sync_order()
    test_kernel_finalizer_sequences_teardown_sync_unmount_and_platform_action()
    test_kernel_thread_shutdown_is_scheduler_owned_not_ntp_specific()
    test_shutdown_unmount_is_exact_deepest_first_and_root_last()
    test_acpi_power_validates_fadt_reset_pm1_and_s5()
    print("shutdown/reboot v1 source invariants hold")


if __name__ == "__main__":
    main()
