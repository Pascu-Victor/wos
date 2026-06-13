#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
KERNEL_ABI = ROOT / "modules" / "kern" / "src" / "abi" / "callnums" / "sys_log.h"
MLIBC_ABI = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "include" / "callnums" / "sys_log.h"
JOURNAL_MAIN = ROOT / "modules" / "journal" / "src" / "main.cpp"
JOURNAL_LIB = ROOT / "modules" / "journal" / "src" / "journal_lib.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|void|int|bool)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
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


def constant_value(source: str, name: str) -> str:
    match = re.search(rf"\bconstexpr\s+(?:uint\d+_t|size_t|auto)\s+{name}\s*=\s*([^;]+);", source)
    if match is None:
        fail(f"missing constant {name}")
    return re.sub(r"\s+", " ", match.group(1).strip())


def journal_record_fields(source: str) -> list[str]:
    match = re.search(r"struct\s+__attribute__\(\(packed\)\)\s+JournalRecord\s*\{(?P<body>.*?)\};", source, flags=re.DOTALL)
    if match is None:
        fail("missing packed JournalRecord")
    fields: list[str] = []
    for raw_line in match.group("body").splitlines():
        line = raw_line.split("//", 1)[0].strip()
        if not line:
            continue
        fields.append(re.sub(r"\s+", " ", line))
    return fields


def test_journal_record_abi_is_shared_and_pinned() -> None:
    kernel = KERNEL_ABI.read_text()
    mlibc = MLIBC_ABI.read_text()

    for name in [
        "JOURNAL_RECORD_MAGIC",
        "JOURNAL_RECORD_VERSION",
        "JOURNAL_MODULE_MAX",
        "JOURNAL_MESSAGE_MAX",
    ]:
        if constant_value(kernel, name) != constant_value(mlibc, name):
            fail(f"{name} differs between kernel and mlibc ABI headers")

    if journal_record_fields(kernel) != journal_record_fields(mlibc):
        fail("JournalRecord field order/types differ between kernel and mlibc ABI headers")

    require_tokens(
        kernel,
        [
            "static_assert(sizeof(JournalRecord) == 608",
            "offsetof(JournalRecord, magic) == 0",
            "offsetof(JournalRecord, header_size) == 6",
            "offsetof(JournalRecord, module) == 60",
            "offsetof(JournalRecord, message_len) == 92",
            "offsetof(JournalRecord, message) == 96",
        ],
        "kernel JournalRecord ABI assertions",
    )


def test_journal_tool_validates_and_filters_records() -> None:
    source = JOURNAL_MAIN.read_text()
    require_tokens(
        source,
        [
            'constexpr const char* JOURNAL_DEVICE = "/dev/journal"',
            'constexpr const char* JOURNAL_FILE = "/var/log/journal/wos.journal"',
            "constexpr uint16_t JOURNAL_HEADER_SIZE = sizeof(JournalRecord) - ker::abi::sys_log::JOURNAL_MESSAGE_MAX",
            "constexpr uint32_t FLAG_KERNEL = 1U << 1",
        ],
        "journal constants",
    )

    valid_body = function_body(source, "valid_record")
    require_tokens(
        valid_body,
        [
            "rec.magic != ker::abi::sys_log::JOURNAL_RECORD_MAGIC",
            "rec.version != ker::abi::sys_log::JOURNAL_RECORD_VERSION",
            "rec.header_size != JOURNAL_HEADER_SIZE",
            "rec.level > 7",
            "rec.message_len >= ker::abi::sys_log::JOURNAL_MESSAGE_MAX",
            "bounded_string_length(rec.module, ker::abi::sys_log::JOURNAL_MODULE_MAX)",
            "bounded_string_length(rec.message, static_cast<size_t>(rec.message_len) + 1) == rec.message_len",
        ],
        "journal valid_record",
    )

    matches_body = function_body(source, "record_matches")
    require_tokens(
        matches_body,
        [
            "if (!valid_record(rec))",
            "rec.level < opts.min_level",
            "opts.kernel_only && (rec.flags & FLAG_KERNEL) == 0",
            "std::strcmp(rec.module, opts.module) != 0",
            "opts.since_us != 0 && rec.monotonic_us < opts.since_us",
        ],
        "journal record_matches",
    )

    load_body = function_body(source, "load_records_from_fd")
    require_tokens(
        load_body,
        [
            "read_journal_record(fd, rec)",
            "if (valid_record(rec))",
        ],
        "journal record loading",
    )


def test_journal_io_retries_interrupted_syscalls() -> None:
    source = JOURNAL_MAIN.read_text()

    sleep_body = function_body(source, "sleep_short")
    require_tokens(
        sleep_body,
        [
            "timespec remaining",
            "nanosleep(&remaining, &remaining) < 0 && errno == EINTR",
        ],
        "journal interrupted sleep",
    )

    write_body = function_body(source, "write_all")
    require_tokens(
        write_body,
        [
            "write(fd, p + done, len - done)",
            "N < 0 && errno == EINTR",
            "continue;",
            "done += static_cast<size_t>(N)",
        ],
        "journal interrupted writes",
    )

    read_record_body = function_body(source, "read_journal_record")
    require_tokens(
        read_record_body,
        [
            "read(fd, out + done, sizeof(rec) - done)",
            "N < 0 && errno == EINTR",
            "continue;",
            "N <= 0",
            "done += static_cast<size_t>(N)",
        ],
        "journal interrupted record reads",
    )

    read_batch_body = function_body(source, "read_journal_batch")
    require_tokens(
        read_batch_body,
        [
            "read(fd, batch.data(), batch.size() * sizeof(JournalRecord))",
            "N < 0 && errno == EINTR",
            "continue;",
            "count = static_cast<size_t>(N) / sizeof(JournalRecord)",
            "return count > 0",
        ],
        "journal interrupted batch reads",
    )


def test_journal_daemon_and_cli_dispatch_are_covered() -> None:
    source = JOURNAL_MAIN.read_text()
    parse_body = function_body(source, "parse_args")
    require_tokens(
        parse_body,
        [
            'ARG == "--daemon"',
            'ARG == "-f"',
            'ARG == "-k"',
            'ARG == "-p"',
            'ARG == "-u" || ARG == "-m"',
            'ARG == "-n"',
            'ARG == "--since"',
        ],
        "journal CLI parser",
    )

    daemon_body = function_body(source, "run_daemon")
    require_tokens(
        daemon_body,
        [
            "open(JOURNAL_DEVICE, O_RDONLY)",
            "read_journal_batch(DEV, batch, records)",
            "if (!valid_record(rec))",
            "open_journal_file_append()",
            "persist_record(out, rec)",
        ],
        "journald persistence loop",
    )

    query_body = function_body(source, "run_query")
    require_tokens(
        query_body,
        [
            "load_records_from_fd(FILE, records)",
            "load_records_from_fd(DEV, live)",
            "read_journal_batch(DEV, batch, records)",
        ],
        "journal query/follow reads",
    )

    main_body = function_body(source, "main")
    require_tokens(
        main_body,
        [
            'std::strcmp(base_name(argv[0]), "journald") == 0',
            "opts.daemon = true",
            "return run_daemon()",
            "return run_query(opts)",
        ],
        "journal basename dispatch",
    )


def test_libjournal_exports_match_syslog_ops() -> None:
    source = JOURNAL_LIB.read_text()
    require_tokens(
        source,
        [
            "visibility(\"default\")",
            "auto log(const char* str, uint64_t len, abi::sys_log::sys_log_device device)",
            "auto logLine(const char* str, uint64_t len, abi::sys_log::sys_log_device device)",
            "auto logEx(const char* module, abi::sys_log::sys_log_level level, const char* str",
            "auto beginLogBlock() -> uint64_t",
            "auto endLogBlock(uint64_t cookie) -> uint64_t",
            "auto logBlock(uint64_t cookie, const char* str, uint64_t len",
            "auto logLineBlock(uint64_t cookie, const char* str, uint64_t len",
            "auto logExBlock(uint64_t cookie, const char* module",
            "abi::sys_log::sys_log_ops::LOG",
            "abi::sys_log::sys_log_ops::LOG_LINE",
            "abi::sys_log::sys_log_ops::LOG_EX",
            "abi::sys_log::sys_log_ops::LOG_BLOCK_BEGIN",
            "abi::sys_log::sys_log_ops::LOG_BLOCK_END",
        ],
        "libjournal exported wrappers",
    )


def main() -> None:
    test_journal_record_abi_is_shared_and_pinned()
    test_journal_tool_validates_and_filters_records()
    test_journal_io_retries_interrupted_syscalls()
    test_journal_daemon_and_cli_dispatch_are_covered()
    test_libjournal_exports_match_syslog_ops()
    print("journal ABI, CLI, daemon, and libjournal source checks passed")


if __name__ == "__main__":
    main()
