#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MEMACC_IO_CPP = ROOT / "modules" / "memacc" / "src" / "procfs_io.cpp"
MEMACC_IO_HPP = ROOT / "modules" / "memacc" / "src" / "procfs_io.hpp"


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


def test_memacc_reads_are_byte_capped() -> None:
    source = MEMACC_IO_CPP.read_text()
    header = MEMACC_IO_HPP.read_text()
    require_tokens(
        header,
        [
            "READ_CHUNK_CAPACITY",
            "MEMACC_READ_LIMIT",
            "auto read_file(std::string_view path, size_t max_bytes = MEMACC_READ_LIMIT) -> std::optional<std::string>",
        ],
        "memacc bounded read surface",
    )

    read_file_body = function_body(source, "read_file")
    require_tokens(
        read_file_body,
        [
            "max_bytes - out.size()",
            "read(fd.get(), &extra, 1)",
            "COUNT < 0 && errno == EINTR",
            "COUNT < 0 || COUNT > 0",
            "return std::nullopt",
            "read(fd.get(), buf.data(), std::min(buf.size(), REMAINING))",
            "out.append(buf.data(), static_cast<size_t>(COUNT))",
        ],
        "memacc bounded read loop",
    )
    if "char buf[4096]" in read_file_body:
        fail("memacc read_file must not use an uncapped raw buffer loop")


def main() -> None:
    test_memacc_reads_are_byte_capped()
    print("memacc file reads are byte capped")


if __name__ == "__main__":
    main()
