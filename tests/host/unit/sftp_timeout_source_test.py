#!/usr/bin/env python3

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
CLUSTER_SETUP = ROOT / "scripts" / "cluster" / "cluster_setup.py"
BENCH_SUITE = ROOT / "scripts" / "bench" / "run_cross_os_benchmark_suite.py"


def fail(message: str) -> None:
    raise AssertionError(message)


def parse(path: Path) -> ast.Module:
    return ast.parse(path.read_text(), filename=str(path))


def call_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = call_name(node.value)
        return f"{base}.{node.attr}" if base else node.attr
    return None


def find_function(module: ast.Module, name: str) -> ast.FunctionDef:
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    fail(f"missing function {name}")


def require_keyword(call: ast.Call, keyword: str, context: str) -> None:
    if not any(item.arg == keyword for item in call.keywords):
        fail(f"{context} is missing keyword argument {keyword}")


def require_calls_have_timeout(
    module: ast.Module,
    names: set[str],
    context: str,
) -> None:
    for node in ast.walk(module):
        if isinstance(node, ast.Call) and call_name(node.func) in names:
            require_keyword(node, "timeout", context)


def require_parser_option(source: str, option: str, validation: str, context: str) -> None:
    if option not in source:
        fail(f"{context}: missing parser option {option}")
    if validation not in source:
        fail(f"{context}: missing validation {validation}")


def test_cluster_live_sync_sftp_batches_are_bounded() -> None:
    source = CLUSTER_SETUP.read_text()
    module = ast.parse(source, filename=str(CLUSTER_SETUP))

    require_parser_option(
        source,
        "--sync-timeout",
        "--sync-timeout must be nonnegative",
        "cluster live sync timeout",
    )
    if "LIVE_SYNC_SFTP_TIMEOUT_SECONDS" not in source:
        fail("cluster live sync is missing a default SFTP timeout constant")
    if "SftpBatchTimeoutError" not in source:
        fail("cluster live sync is missing a timeout-specific error")

    run_sftp_batch = find_function(module, "run_sftp_batch")
    subprocess_runs = [
        node
        for node in ast.walk(run_sftp_batch)
        if isinstance(node, ast.Call) and call_name(node.func) == "subprocess.run"
    ]
    if not subprocess_runs:
        fail("run_sftp_batch must execute sftp through subprocess.run")
    for call in subprocess_runs:
        require_keyword(call, "timeout", "run_sftp_batch subprocess.run")

    require_calls_have_timeout(
        module,
        {"run_sftp_batch"},
        "cluster run_sftp_batch call",
    )
    for name in ("sync_live_rootfs", "sync_host", "upload_sync_chunk", "upload_manifest"):
        if "sftp_timeout" not in ast.get_source_segment(source, find_function(module, name)):
            fail(f"{name} does not thread sftp_timeout")


def test_cross_os_artifact_fetches_are_bounded() -> None:
    source = BENCH_SUITE.read_text()
    module = ast.parse(source, filename=str(BENCH_SUITE))

    require_parser_option(
        source,
        "--artifact-fetch-timeout",
        "--artifact-fetch-timeout must be nonnegative",
        "cross-os benchmark artifact fetch timeout",
    )
    if "DEFAULT_ARTIFACT_FETCH_TIMEOUT_SECONDS" not in source:
        fail("cross-os benchmark suite is missing a default artifact fetch timeout")

    fetch_remote_file = find_function(module, "fetch_remote_file")
    run_command_calls = [
        node
        for node in ast.walk(fetch_remote_file)
        if isinstance(node, ast.Call) and call_name(node.func) == "run_command"
    ]
    if not run_command_calls:
        fail("fetch_remote_file must call run_command")
    for call in run_command_calls:
        require_keyword(call, "timeout", "fetch_remote_file run_command")

    require_calls_have_timeout(
        module,
        {"fetch_remote_file", "fetch_optional_remote_file"},
        "cross-os artifact fetch call",
    )


def test_cross_os_optimal_render_uses_rollbackable_persistent_workers() -> None:
    source = BENCH_SUITE.read_text()
    module = ast.parse(source, filename=str(BENCH_SUITE))

    run_wos_renderbench = find_function(module, "run_wos_renderbench")
    body = ast.get_source_segment(source, run_wos_renderbench)
    if body is None:
        fail("missing run_wos_renderbench source segment")

    disable_branch = "if args.wos_disable_process_persistent_workers:"
    enable_branch = 'elif args.wos_enable_process_persistent_workers or args.wos_render_tuning == "optimal":'
    if disable_branch not in body or enable_branch not in body:
        fail("WOS optimal render persistence policy or explicit rollback is missing")
    if body.index(disable_branch) >= body.index(enable_branch):
        fail("WOS explicit persistent-worker rollback must take precedence over optimal tuning")
    for flag in (
        'command += ["--disable-process-persistent-workers"]',
        'command += ["--enable-process-persistent-workers"]',
    ):
        if flag not in body:
            fail(f"WOS renderbench persistence policy no longer passes {flag}")


def main() -> None:
    test_cluster_live_sync_sftp_batches_are_bounded()
    test_cross_os_artifact_fetches_are_bounded()
    test_cross_os_optimal_render_uses_rollbackable_persistent_workers()
    print("SFTP automation paths are timeout bounded")


if __name__ == "__main__":
    main()
