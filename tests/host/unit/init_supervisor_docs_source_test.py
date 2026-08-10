#!/usr/bin/env python3

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DOC = ROOT / "docs" / "init_supervisor.md"
MANIFEST_HEADER = ROOT / "modules" / "init" / "src" / "service_manifest.h"
MANIFEST_IMPL = ROOT / "modules" / "init" / "src" / "service_manifest.cpp"
MODEL_HEADER = ROOT / "modules" / "init" / "src" / "supervisor_model.h"
MODEL_IMPL = ROOT / "modules" / "init" / "src" / "supervisor_model.cpp"
RUNTIME = ROOT / "modules" / "init" / "src" / "services.cpp"
SHUTDOWN = ROOT / "modules" / "init" / "src" / "shutdown.cpp"
CONTROL_ABI = ROOT / "modules" / "kern" / "src" / "abi" / "init_control.hpp"
SERVICECTL = ROOT / "modules" / "servicectl" / "src" / "main.cpp"
KTEST = ROOT / "bin" / "wos-ktest"
CLUSTER = ROOT / "bin" / "wos-cluster"
CLUSTER_CONFIG = ROOT / "configs" / "cluster_selfhost.json"


def fail(message: str) -> None:
    raise AssertionError(message)


def require_all(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(repr(token) for token in missing)}")


def main() -> None:
    source = DOC.read_text(encoding="utf-8")
    normalized = " ".join(source.split())
    manifest_header = MANIFEST_HEADER.read_text(encoding="utf-8")
    manifest_impl = MANIFEST_IMPL.read_text(encoding="utf-8")
    model_header = MODEL_HEADER.read_text(encoding="utf-8")
    model_impl = MODEL_IMPL.read_text(encoding="utf-8")
    runtime = RUNTIME.read_text(encoding="utf-8")
    shutdown = SHUTDOWN.read_text(encoding="utf-8")
    control_abi = CONTROL_ABI.read_text(encoding="utf-8")
    servicectl = SERVICECTL.read_text(encoding="utf-8")
    ktest = KTEST.read_text(encoding="utf-8")
    cluster = CLUSTER.read_text(encoding="utf-8")
    if not CLUSTER_CONFIG.is_file():
        fail("documented rootless cluster config is missing")

    require_all(
        manifest_header,
        [
            "SERVICE_MANIFEST_VERSION = 1",
            "MAX_MANIFEST_BYTES = 32 * 1024",
            "MAX_MANIFEST_LINE_BYTES = 512",
            "MAX_SERVICES = 16",
            "MAX_ARGUMENTS = 16",
            "MAX_ENVIRONMENT = 16",
            "MAX_DEPENDENCIES = 8",
        ],
        "manifest bounds source",
    )
    require_all(
        manifest_impl,
        [
            '"immediate"sv',
            '"ipv4"sv',
            '"exit-success"sv',
            '"path-exists"sv',
            '"path-missing"sv',
            "MAX_RESTART_BUDGET = 32",
            "MAX_STOP_TIMEOUT_MS = 60U * 1000U",
        ],
        "manifest policy source",
    )
    require_all(
        model_header,
        [
            "WAITING = 0",
            "READY = 3",
            "BACKOFF = 7",
            "DISABLED = 8",
            "bool abandoned{}",
            "NON_JOURNAL_SHUTDOWN_COMPLETE",
        ],
        "supervisor model contract",
    )
    require_all(
        model_impl,
        [
            "lifecycle.abandoned = true",
            "lifecycle.state == ServiceState::FAILED && lifecycle.abandoned",
            "SupervisorControl::START && spec.enablement == EnablementKind::DISABLED",
            "!lifecycle.condition_met && !MANUAL_ENABLEMENT",
            "case SupervisorEventKind::PROBE_ERROR:",
            "TransitionReason::READINESS_LOST",
            "TransitionReason::RESTART_BUDGET_EXHAUSTED",
        ],
        "supervisor lifecycle policy",
    )
    require_all(
        runtime,
        [
            "FD_CLOEXEC",
            "setpgid(0, 0)",
            "setpgid(PID, PID)",
            "kill(-slot.pgid",
            "waitpid(-1, &status, WNOHANG",
            "OUTPUT_READ_BYTES_PER_TICK = 4096",
            "!slot.leader_reaped",
            "!slot.exec_resolved",
            "!slot.output_complete",
            "!service_group_gone(slot)",
            "init_control_receive",
            "init_status_publish",
            "SERVICE_WKI_TARGET_FLAGS",
        ],
        "PID 1 integration source",
    )
    require_all(
        shutdown,
        [
            "stop_services_for_shutdown()",
            "prepare_kernel_shutdown()",
            "sync_vfs()",
            "stop_journald_for_shutdown()",
        ],
        "shutdown integration source",
    )
    shutdown_body = shutdown[shutdown.index("void shutdown_perform(") :]
    shutdown_steps = [
        shutdown_body.index("stop_services_for_shutdown()"),
        shutdown_body.index("prepare_kernel_shutdown()"),
        shutdown_body.index("sync_vfs()"),
        shutdown_body.index("stop_journald_for_shutdown()"),
    ]
    if shutdown_steps != sorted(shutdown_steps):
        fail("shutdown integration source: journald is not stopped after first sync")
    if shutdown_body.find("sync_vfs()", shutdown_steps[-1]) < 0:
        fail("shutdown integration source: final sync after journald stop is missing")
    require_all(
        control_abi,
        [
            "MAILBOX_CAPACITY = 16",
            "MAX_TRANSITION_HISTORY = 4",
            "START = 1",
            "STOP = 2",
            "RESTART = 3",
        ],
        "control ABI source",
    )
    require_all(
        servicectl,
        [
            '"status"',
            '"start"',
            '"stop"',
            '"restart"',
            "accepted request=%llu service=%s",
        ],
        "servicectl source",
    )
    require_all(
        ktest, ['"--no-setup"', '"--debug-node"'], "KTEST rootless debug options"
    )
    require_all(
        cluster, ['"--no-setup"', '"--debug-node"'], "cluster rootless debug options"
    )

    require_all(
        normalized,
        [
            "version=1",
            "32 KiB per manifest",
            "512 bytes per physical line",
            "16 services",
            "`1`–`600000`",
            "`1`–`3600000`",
            "`1`–`32`",
            "`1`–`86400000`",
            "`1`–`60000`",
            "The first `arg` must equal `exec`",
            "Every other enabled service must depend on the journal transitively",
        ],
        "versioned bounded grammar",
    )
    require_all(
        normalized,
        [
            "process group",
            "close-on-exec",
            "generation-fenced `QUIESCED` barrier",
            "`FAILED` with an abandoned generation",
            "explicitly enables an `enable=disabled` service",
            "probe error withdraws IPv4 readiness",
            "TERM is followed by KILL",
            "journald is last",
            "effective UID 0",
            "accepted request=...",
            "does **not** mean",
            "mailbox holds 16 requests",
        ],
        "runtime and control invariants",
    )
    require_all(
        normalized,
        [
            "bin/wos-ktest --no-setup",
            "bin/wos-ktest --no-build --no-package --no-setup",
            "bin/wos-ktest --debug-node --no-setup",
            "bin/wos-cluster --config configs/cluster_selfhost.json --launch --no-setup",
            "bin/wos-cluster --config configs/cluster_selfhost.json --launch --debug-node 0 --no-setup",
            "Host/model evidence",
            "Live evidence required",
            "No live boot or fault-campaign success is claimed here",
            "Missing executable",
            "Crash loop",
            "Delayed IPv4",
            "Missing IPv4",
            "Control/drain race",
            "Shutdown during transition",
        ],
        "rootless validation and bounded evidence matrix",
    )

    for line in source.splitlines():
        command = line.strip()
        if command.startswith("bin/wos-ktest") and "--no-setup" not in command:
            fail(f"KTEST launch example is not rootless: {command!r}")
        if command.startswith("bin/wos-cluster") and "--no-setup" not in command:
            fail(f"cluster launch example is not rootless: {command!r}")

    print("init supervisor documentation invariants hold")


if __name__ == "__main__":
    main()
