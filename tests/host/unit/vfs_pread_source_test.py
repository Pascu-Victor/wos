#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
CORE_CPP = ROOT / "modules" / "kern" / "src" / "vfs" / "core.cpp"
VFS_KTEST_CPP = ROOT / "modules" / "kern" / "src" / "test" / "vfs_ktest.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\bauto\s+{re.escape(name)}\([^)]*\)\s*->\s*[A-Za-z0-9_:<>*]+(?:\s+const)?\s*\{{", source)
    if match is None:
        match = re.search(rf"\bKTEST\([^,]+,\s*{re.escape(name)}\)\s*\{{", source)
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


def require_order(body: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = body.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token!r}")
        cursor = found + len(token)


def test_pread_paths_bypass_stream_cache() -> None:
    source = CORE_CPP.read_text()
    for name in ("vfs_pread", "vfs_pread_file"):
        body = function_body(source, name)
        if "vfs_stream_cache_try_read" in body:
            fail(f"{name} must not use the stream cache for positional reads")
        if "g_vfs_stream_misses.fetch_add" in body:
            fail(f"{name} must not account positional reads as stream-cache misses")
        require_order(
            body,
            [
                "positional_read_depth.fetch_add(1, std::memory_order_acq_rel)",
                "f->fops->vfs_read(f, buf, count, static_cast<size_t>(offset))",
                "positional_read_depth.fetch_sub(1, std::memory_order_acq_rel)",
            ],
            name,
        )


def test_ktest_covers_pread_contract() -> None:
    source = VFS_KTEST_CPP.read_text()
    if "KTEST(VFS, PreadBypassesStreamCacheAndPreservesOffset)" not in source:
        fail("vfs_ktest must cover pread stream-cache bypass")
    body = function_body(source, "PreadBypassesStreamCacheAndPreservesOffset")
    required = [
        "vfs_pread_file(rf, buf, sizeof(buf), 0)",
        "KEXPECT_EQ(rf->pos, 0)",
        "KEXPECT_EQ(after.stream_backend_reads, before.stream_backend_reads)",
        "KEXPECT_EQ(after.stream_hits, before.stream_hits)",
        "KEXPECT_EQ(after.stream_copied_bytes, before.stream_copied_bytes)",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("pread KTEST is missing expected assertions: " + ", ".join(missing))


def test_local_xfs_reads_bypass_stream_cache() -> None:
    source = CORE_CPP.read_text()
    body = function_body(source, "stream_cache_read_eligible")

    if "file->fs_type == FSType::XFS" in body:
        fail("local XFS reads must not enter the VFS stream cache")
    if "file->fs_type == FSType::REMOTE" not in body:
        fail("remote VFS reads should remain stream-cache eligible")

    selftest_body = function_body(source, "vfs_selftest_stream_cache_read_eligibility")
    required = [
        "bool const LOCAL_REGULAR_REJECTED = !stream_cache_read_eligible(&xfs_read)",
        "bool const REMOTE_ALLOWED = stream_cache_read_eligible(&remote_read)",
        "return LOCAL_REGULAR_REJECTED &&",
    ]
    missing = [token for token in required if token not in selftest_body]
    if missing:
        fail("stream-cache eligibility selftest is missing expected assertions: " + ", ".join(missing))


def test_loader_path_trace_is_compile_time_disabled_by_default() -> None:
    source = CORE_CPP.read_text()
    required = [
        "inline constexpr bool ENABLE_LOADER_PATH_TRACE = false;",
        "void log_loader_path_event(",
        "if constexpr (!ENABLE_LOADER_PATH_TRACE) {",
        'log::info("loader-path:',
    ]
    missing = [token for token in required if token not in source]
    if missing:
        fail("loader-path trace guard is missing expected tokens: " + ", ".join(missing))

    function_start = source.find("void log_loader_path_event(")
    guard = source.find("if constexpr (!ENABLE_LOADER_PATH_TRACE)", function_start)
    log_call = source.find('log::info("loader-path:', function_start)
    if function_start < 0 or guard < 0 or log_call < 0 or not (function_start < guard < log_call):
        fail("loader-path INFO logging must stay behind the compile-time trace guard")


def main() -> None:
    test_pread_paths_bypass_stream_cache()
    test_ktest_covers_pread_contract()
    test_local_xfs_reads_bypass_stream_cache()
    test_loader_path_trace_is_compile_time_disabled_by_default()
    print("VFS pread source invariants hold")


if __name__ == "__main__":
    main()
