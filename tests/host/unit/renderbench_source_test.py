#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
RENDERBENCH_MAIN_CPP = ROOT / "modules" / "renderbench" / "src" / "main.cpp"


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


def require_order(source: str, before: str, after: str, context: str) -> None:
    before_pos = source.find(before)
    after_pos = source.find(after)
    if before_pos < 0 or after_pos < 0 or before_pos >= after_pos:
        fail(f"{context}: expected {before!r} before {after!r}")


def test_renderbench_worker_reap_is_deadline_bounded() -> None:
    source = RENDERBENCH_MAIN_CPP.read_text()
    require_tokens(
        source,
        [
            "WORKER_EXIT_KILL_AFTER_SECONDS",
            "WORKER_EXIT_GIVE_UP_AFTER_SECONDS",
            "sleep_worker_wait_poll",
        ],
        "renderbench worker reap timeout surface",
    )

    wait_body = function_body(source, "wait_for_child")
    if "::waitpid(worker.pid, &status, 0)" in wait_body:
        fail("renderbench wait_for_child must not use a blocking waitpid")

    require_tokens(
        wait_body,
        [
            "::waitpid(worker.pid, &status, WNOHANG)",
            "WAITED == worker.pid",
            "note_worker_exit_status(worker, status, cancellation_expected)",
            "WORKER_EXIT_KILL_AFTER_SECONDS",
            "::kill(worker.pid, SIGKILL)",
            "WORKER_EXIT_GIVE_UP_AFTER_SECONDS",
            "sleep_worker_wait_poll()",
        ],
        "renderbench bounded wait_for_child",
    )

    children_body = function_body(source, "wait_for_children")
    require_tokens(
        children_body,
        ["wait_for_child(worker, cancellation_expected)"],
        "renderbench wait_for_children plumbing",
    )


def test_renderbench_worker_cancellation_is_cooperative() -> None:
    source = RENDERBENCH_MAIN_CPP.read_text()
    main_body = function_body(source, "main")
    require_order(
        main_body,
        "install_cancel_signal_handlers();",
        "if (worker.enabled)",
        "renderbench worker mode must install signal handlers before entering worker loop",
    )

    for name in ["read_exact", "write_all", "render_worker_tile", "worker_thread", "command_stream_worker_thread"]:
        body = function_body(source, name)
        if "cancel_requested()" not in body:
            fail(f"{name} must observe cooperative cancellation")


def test_renderbench_worker_stream_corruption_is_fatal() -> None:
    source = RENDERBENCH_MAIN_CPP.read_text()
    body = function_body(source, "parse_worker_packets")
    if "resynchronized" in body:
        fail("renderbench must not resynchronize through corrupt worker byte streams")
    require_tokens(
        body,
        [
            "corrupt worker stream",
            "worker.done_failed = true;",
            "return false;",
        ],
        "renderbench corrupt worker stream handling",
    )


def test_renderbench_distributed_ipc_uses_chunk_safe_default_tiles() -> None:
    source = RENDERBENCH_MAIN_CPP.read_text()
    require_tokens(
        source,
        [
            "IPC_SAFE_DEFAULT_TILE_SIZE = 24",
            "apply_distributed_ipc_defaults",
            "options.tile_size_explicit",
        ],
        "renderbench distributed IPC tile-size default",
    )

    main_body = function_body(source, "main")
    require_order(
        main_body,
        "apply_distributed_ipc_defaults(options);",
        "return run_distributed_ipc(options, peers, argv[0]);",
        "renderbench must apply distributed IPC defaults before launching workers",
    )


def main() -> None:
    test_renderbench_worker_reap_is_deadline_bounded()
    test_renderbench_worker_cancellation_is_cooperative()
    test_renderbench_worker_stream_corruption_is_fatal()
    test_renderbench_distributed_ipc_uses_chunk_safe_default_tiles()
    print("renderbench worker source invariants hold")


if __name__ == "__main__":
    main()
