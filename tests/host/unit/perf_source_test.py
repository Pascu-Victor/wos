#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PERF_CPP = ROOT / "modules" / "perf" / "src" / "main.cpp"


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


def test_perf_reads_are_byte_capped() -> None:
    source = PERF_CPP.read_text()
    require_tokens(
        source,
        [
            "READ_CHUNK_CAPACITY",
            "PROCFS_READ_LIMIT",
            "PERF_PROC_READ_LIMIT",
            "PERF_DATA_READ_LIMIT",
            "auto read_fd(ScopedFd& fd, std::size_t initial_capacity = INITIAL_FILE_CAPACITY, std::size_t max_bytes = PERF_DATA_READ_LIMIT)",
            "-> std::optional<std::string>",
        ],
        "perf bounded read surface",
    )

    limit_body = function_body(source, "read_limit_for_path")
    require_tokens(
        limit_body,
        [
            "path == KPERF_PATH",
            "path == KWKISTAT_PATH",
            "path == KIPCSTAT_PATH",
            "path == KCPUSTAT_PATH",
            "path == KCONTSTAT_PATH",
            "return PERF_PROC_READ_LIMIT",
            "path.starts_with(PROC_ROOT)",
            "path.starts_with(DEV_NODES_ROOT)",
            "return PROCFS_READ_LIMIT",
            "return PERF_DATA_READ_LIMIT",
        ],
        "perf path read caps",
    )

    read_fd_body = function_body(source, "read_fd")
    require_tokens(
        read_fd_body,
        [
            "max_bytes - buffer.size()",
            "read(fd.get(), &extra, 1)",
            "COUNT < 0 && errno == EINTR",
            "COUNT < 0 || COUNT > 0",
            "return std::nullopt",
            "std::min(chunk.size(), REMAINING)",
            "buffer.append(chunk.data(), static_cast<std::size_t>(COUNT))",
        ],
        "perf bounded read loop",
    )
    if "buffer.resize(buffer.size() * 2)" in read_fd_body:
        fail("perf read_fd must not use an unbounded doubling loop")

    read_file_body = function_body(source, "read_file")
    require_tokens(
        read_file_body,
        ["return read_fd(fd, initial_capacity, read_limit_for_path(path));"],
        "perf read_file cap dispatch",
    )


def test_perf_run_waits_for_descendant_process_group() -> None:
    source = PERF_CPP.read_text()
    cmd_run_body = function_body(source, "cmd_run")
    require_tokens(
        cmd_run_body,
        [
            "ker::process::setpgid(0, 0)",
            "ker::process::setpgid(child_pid, child_pid)",
            "int64_t target_pgid = child_pid",
            "std::cmp_equal(stat.pgid, target_pgid)",
            "last_group_alive = any_alive",
            "command_exited = true",
            "if (command_exited && !last_group_alive)",
            "set_recording_enabled(false)",
        ],
        "perf run process-group tracing",
    )
    if "if (command_exited || !any_alive)" in cmd_run_body:
        fail("perf run must not stop recording while same-PGID descendants are still alive")
    if "if (command_exited || !last_group_alive)" in cmd_run_body:
        fail("perf run must not stop recording while same-PGID descendants are still alive")


def main() -> None:
    test_perf_reads_are_byte_capped()
    test_perf_run_waits_for_descendant_process_group()
    print("perf file reads are byte capped")


if __name__ == "__main__":
    main()
