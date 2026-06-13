#!/usr/bin/env python3
"""
ktest_cov.py — Symbolize ktest KCOV PC dump into a coverage report.

Usage:
    wos-ktest-cov [options]

    -e / --elf     Kernel ELF  (default: build-ktest/modules/kern/wos)
    -l / --log     Serial log  (default: ktest-data/serial-vm0.log)
    --lcov FILE    Also write an lcov .info file (load with genhtml)
    --src PATH     Filter to source files containing PATH
    --require-100  Fail unless total and per-file line coverage are 100%

Serial log lines look like (journal format):
    [T.mmm] INFO kcov_elf: text_start=0x... text_end=0x... text_fnv64=0x...
    [T.mmm] INFO kcov_begin: <unique_count>
    [T.mmm] INFO kcov_truncated: count=<recorded> capacity=<capacity>
    [T.mmm] INFO kcov: 0x... 0x... 0x... 0x... 0x... 0x... 0x... 0x...
    [T.mmm] INFO kcov_end:

After generating an lcov file:
    genhtml coverage.info -o coverage-html/
    xdg-open coverage-html/index.html
"""

import argparse
import dataclasses
import fnmatch
import hashlib
import pickle
import re
import shlex
import subprocess
import sys
import tempfile
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
EXCLUDED_SOURCE_PREFIXES = (
    ROOT / "build",
    ROOT / "build-ktest",
    ROOT / "ktest-data",
    ROOT / "toolchain" / "host",
    ROOT / "toolchain" / "sysroot",
)
CACHE_DIR = ROOT / "ktest-data" / ".ktest_cov_cache"
EXCLUDED_FUNCTION_PREFIXES = (
    "__cfi__",
    "__cfi_",
)
UNCOVERABLE_SOURCE_SUFFIXES = (
    ".asm",
    ".s",
    ".S",
)
KCOV_ELF_RE = re.compile(
    r"(?:kcov_elf:|\[kcov_elf\])\s+"
    r"text_start=(0x[0-9a-fA-F]+)\s+"
    r"text_end=(0x[0-9a-fA-F]+)\s+"
    r"text_fnv64=(0x[0-9a-fA-F]+)",
    re.IGNORECASE,
)
KCOV_BEGIN_RE = re.compile(r"(?:kcov_begin:|\[kcov_begin\])\s*(?P<count>\d+)", re.IGNORECASE)


class CoverageLogError(RuntimeError):
    pass


@dataclasses.dataclass(frozen=True)
class TextFingerprint:
    text_start: int
    text_end: int
    text_fnv64: int


@dataclasses.dataclass(frozen=True)
class CoverageSummary:
    files: int
    hit_lines: int
    total_lines: int

    @property
    def percent(self) -> float:
        return (100.0 * self.hit_lines / self.total_lines) if self.total_lines else 0.0


@dataclasses.dataclass(frozen=True)
class KcovBlock:
    pcs: list[int]
    fingerprint: TextFingerprint | None
    truncated: bool
    declared_count: int | None


@dataclasses.dataclass(frozen=True, order=True)
class SymbolizedLocation:
    function: str
    file: str
    line: int


@dataclasses.dataclass(frozen=True)
class TextSymbol:
    name: str
    address: int
    size: int


@dataclasses.dataclass(frozen=True)
class FunctionRecord:
    line: int
    name: str
    hit_count: int = 0
    end_line: int | None = None


@dataclasses.dataclass(frozen=True)
class UncoverableRange:
    start_line: int
    end_line: int
    reason: str


@dataclasses.dataclass(frozen=True)
class SanitizerIgnorelist:
    source_patterns: tuple[str, ...]
    function_patterns: tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class CoverageExclusions:
    lines_by_file: dict[str, dict[int, str]]
    functions_by_file: dict[str, dict[str, str]]

    @property
    def line_count(self) -> int:
        return sum(len(lines) for lines in self.lines_by_file.values())

    @property
    def function_count(self) -> int:
        return sum(len(functions) for functions in self.functions_by_file.values())


def parse_kcov_elf_line(line: str) -> TextFingerprint | None:
    match = KCOV_ELF_RE.search(line)
    if match is None:
        return None
    return TextFingerprint(
        text_start=int(match.group(1), 16),
        text_end=int(match.group(2), 16),
        text_fnv64=int(match.group(3), 16),
    )


def format_fingerprint(fingerprint: TextFingerprint) -> str:
    return (
        f"text_start=0x{fingerprint.text_start:x} "
        f"text_end=0x{fingerprint.text_end:x} "
        f"text_fnv64=0x{fingerprint.text_fnv64:x}"
    )


def extract_latest_kcov_report(log_path: str) -> KcovBlock:
    latest = KcovBlock([], None, False, None)
    current_pcs: list[int] = []
    current_fingerprint: TextFingerprint | None = None
    block_fingerprint: TextFingerprint | None = None
    block_truncated = False
    block_declared_count: int | None = None
    in_block = False
    with open(log_path) as f:
        for line in f:
            parsed_fingerprint = parse_kcov_elf_line(line)
            if parsed_fingerprint is not None:
                current_fingerprint = parsed_fingerprint
                continue

            lower = line.lower()
            if "kcov_begin:" in lower or "[kcov_begin]" in lower:
                match = KCOV_BEGIN_RE.search(line)
                if match is None:
                    raise CoverageLogError("KCOV_BEGIN line is missing a declared PC count")
                in_block = True
                current_pcs = []
                block_fingerprint = current_fingerprint
                block_truncated = False
                block_declared_count = int(match.group("count"))
                continue
            if "kcov_end:" in lower or "[kcov_end]" in lower:
                if in_block:
                    latest = KcovBlock(current_pcs, block_fingerprint, block_truncated, block_declared_count)
                in_block = False
                continue
            if in_block and ("kcov_truncated:" in lower or "[kcov_truncated]" in lower):
                block_truncated = True
                continue
            if in_block and (" kcov: " in lower or "[kcov]" in lower):
                # message part follows "kcov: ", may contain 1–8 hex addresses
                msg = line[lower.index(" kcov: ") + len(" kcov: "):] if " kcov: " in lower else line
                for token in msg.replace("[KCOV]", " ").split():
                    if token.startswith("0x"):
                        try:
                            current_pcs.append(int(token, 16))
                        except ValueError:
                            pass
    if in_block:
        raise CoverageLogError("KCOV log has a newer incomplete coverage block")
    return latest


def extract_latest_kcov_block(log_path: str) -> tuple[list[int], TextFingerprint | None]:
    report = extract_latest_kcov_report(log_path)
    return report.pcs, report.fingerprint


def extract_pcs(log_path: str) -> list[int]:
    pcs, _ = extract_latest_kcov_block(log_path)
    return pcs


def fnv1a64(data: bytes) -> int:
    hash_value = 1469598103934665603
    for byte in data:
        hash_value ^= byte
        hash_value = (hash_value * 1099511628211) & 0xFFFFFFFFFFFFFFFF
    return hash_value


def elf_symbol_addresses(elf: str, symbol_names: set[str]) -> dict[str, int]:
    result = subprocess.run(
        ["llvm-nm", "-P", elf],
        capture_output=True,
        text=True,
        check=True,
    )
    addresses: dict[str, int] = {}
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) < 3 or parts[0] not in symbol_names:
            continue
        try:
            addresses[parts[0]] = int(parts[2], 16)
        except ValueError:
            continue

    missing = sorted(symbol_names - addresses.keys())
    if missing:
        raise RuntimeError(f"{elf}: missing ELF symbol(s): {', '.join(missing)}")
    return addresses


def elf_text_fingerprint(elf: str) -> TextFingerprint:
    symbols = elf_symbol_addresses(elf, {"__kernel_text_start", "__kernel_text_end"})
    with tempfile.TemporaryDirectory(prefix="ktest-cov-") as tmp_dir:
        text_path = Path(tmp_dir) / "text.bin"
        subprocess.run(
            ["llvm-objcopy", f"--dump-section", f".text={text_path}", elf],
            capture_output=True,
            text=True,
            check=True,
        )
        text_bytes = text_path.read_bytes()

    return TextFingerprint(
        text_start=symbols["__kernel_text_start"],
        text_end=symbols["__kernel_text_end"],
        text_fnv64=fnv1a64(text_bytes),
    )


def validate_log_elf_match(log_fingerprint: TextFingerprint | None, elf: str, allow_mismatch: bool, require_fingerprint: bool) -> None:
    if log_fingerprint is None:
        message = (
            "no KCOV ELF fingerprint found in the log; coverage may be stale "
            "if the kernel was rebuilt after this log was produced."
        )
        if require_fingerprint:
            print(f"ERROR: {message}", file=sys.stderr)
            sys.exit(6)
        print(f"WARNING: {message}", file=sys.stderr)
        return

    elf_fingerprint = elf_text_fingerprint(elf)
    if log_fingerprint == elf_fingerprint:
        print(f"  KCOV log/ELF fingerprint matched ({format_fingerprint(log_fingerprint)})")
        return

    message = (
        "KCOV log/ELF fingerprint mismatch; refusing to symbolize stale coverage.\n"
        f"  log: {format_fingerprint(log_fingerprint)}\n"
        f"  elf: {format_fingerprint(elf_fingerprint)}"
    )
    if allow_mismatch:
        print(f"WARNING: {message}", file=sys.stderr)
        return
    print(f"ERROR: {message}", file=sys.stderr)
    sys.exit(2)


def validate_not_truncated(truncated: bool, allow_truncated: bool) -> None:
    if not truncated:
        return

    message = (
        "KCOV buffer filled during selftests; coverage data may be truncated. "
        "Increase the KTEST VM memory or reduce the selected selftest scope before treating coverage as complete."
    )
    if allow_truncated:
        print(f"WARNING: {message}", file=sys.stderr)
        return
    print(f"ERROR: {message}", file=sys.stderr)
    sys.exit(4)


def validate_declared_count(report: KcovBlock) -> None:
    if report.declared_count is None:
        return
    if report.declared_count == len(report.pcs):
        return
    print(
        f"ERROR: KCOV block declared {report.declared_count} PCs but contained {len(report.pcs)} parsed PCs",
        file=sys.stderr,
    )
    sys.exit(5)


def parse_addr2line_location(location: str) -> tuple[str, int] | None:
    match = re.match(r"^(.*):(\d+)(?::\d+)?$", location)
    if match is None:
        return None
    try:
        lineno = int(match.group(2))
    except ValueError:
        return None
    if lineno <= 0:
        return None
    file_part = match.group(1)
    if "?" in file_part:
        return None
    return file_part, lineno


def is_reportable_function_name(name: str) -> bool:
    return name != "??" and not any(name.startswith(prefix) for prefix in EXCLUDED_FUNCTION_PREFIXES)


def symbolize_address_locations(pcs: list[int], elf: str) -> dict[int, list[SymbolizedLocation]]:
    if not pcs:
        return {}
    addr_input = "\n".join(f"0x{pc:x}" for pc in pcs)
    result = subprocess.run(
        # `-a` makes each address explicit in the output stream so we can keep
        # inline/alias locations grouped to the originating PC.
        ["llvm-addr2line", "-e", elf, "-a", "-f", "-i"],
        input=addr_input, capture_output=True, text=True, check=True,
    )
    locations_by_pc: dict[int, list[SymbolizedLocation]] = {}
    lines = result.stdout.splitlines()
    i = 0
    while i < len(lines):
        if not lines[i].startswith("0x"):
            i += 1
            continue

        try:
            pc = int(lines[i], 16)
        except ValueError:
            i += 1
            continue
        i += 1
        address_locations: list[SymbolizedLocation] = []
        seen_locations: set[SymbolizedLocation] = set()

        while i + 1 < len(lines) and not lines[i].startswith("0x"):
            function = lines[i]
            parsed = parse_addr2line_location(lines[i + 1])
            i += 2
            if parsed is None or not is_reportable_function_name(function):
                continue
            file_part, lineno = parsed
            location = SymbolizedLocation(function, file_part, lineno)
            if location not in seen_locations:
                seen_locations.add(location)
                address_locations.append(location)

        locations_by_pc[pc] = address_locations
    return locations_by_pc


def symbolize_locations(pcs: list[int], elf: str) -> list[SymbolizedLocation]:
    locations: list[SymbolizedLocation] = []
    for address_locations in symbolize_address_locations(pcs, elf).values():
        locations.extend(address_locations)
    return locations


def symbolize(pcs: list[int], elf: str) -> list[tuple[str, int]]:
    return [(location.file, location.line) for location in symbolize_locations(pcs, elf)]


def is_wos_source_path(path: Path) -> bool:
    try:
        resolved = path.resolve(strict=False)
        resolved.relative_to(ROOT)
    except ValueError:
        return False
    return not any(resolved.is_relative_to(prefix) for prefix in EXCLUDED_SOURCE_PREFIXES)


def compiled_lines_cache_path(elf: str) -> Path:
    elf_path = Path(elf).resolve(strict=False)
    stat = elf_path.stat()
    key = hashlib.sha256(
        f"{elf_path}:{stat.st_size}:{stat.st_mtime_ns}".encode("utf-8")
    ).hexdigest()
    return CACHE_DIR / f"compiled-lines-{key}.pickle"


def parse_dwarf_file_table(lines: list[str], start: int) -> tuple[int, dict[int, str]]:
    include_dirs: dict[int, str] = {}
    files: dict[int, dict[str, int | str | None]] = {}
    i = start

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()
        if stripped.startswith("debug_line[") and i != start:
            break
        if stripped.startswith("include_directories["):
            prefix, _, value = stripped.partition("=")
            dir_index = int(prefix[prefix.index("[") + 1:prefix.index("]")])
            include_dirs[dir_index] = value.strip().strip('"')
            i += 1
            continue
        if stripped.startswith("file_names["):
            file_index = int(stripped[stripped.index("[") + 1:stripped.index("]")])
            file_name: str | None = None
            dir_index: int | None = None
            i += 1
            while i < len(lines):
                detail = lines[i].strip()
                if detail.startswith("name:"):
                    file_name = detail.split(":", 1)[1].strip().strip('"')
                elif detail.startswith("dir_index:"):
                    dir_index = int(detail.split(":", 1)[1].strip(), 0)
                elif not detail or detail.startswith("file_names[") or detail.startswith("Address") or detail.startswith("debug_line["):
                    break
                i += 1
            files[file_index] = {"name": file_name, "dir_index": dir_index}
            continue
        if stripped.startswith("Address"):
            break
        i += 1

    resolved_files: dict[int, str] = {}
    for file_index, info in files.items():
        name = info["name"]
        if not isinstance(name, str) or not name:
            continue
        path = Path(name)
        if not path.is_absolute():
            dir_index = info["dir_index"]
            if isinstance(dir_index, int) and dir_index in include_dirs:
                path = Path(include_dirs[dir_index]) / path
            else:
                path = ROOT / path
        resolved = path.resolve(strict=False)
        if is_wos_source_path(resolved):
            resolved_files[file_index] = str(resolved)
    return i, resolved_files


def collect_compiled_lines_uncached(elf: str) -> dict[str, set[int]]:
    result = subprocess.run(
        ["llvm-dwarfdump", "--debug-line", elf],
        capture_output=True,
        text=True,
        check=True,
    )

    compiled: dict[str, set[int]] = defaultdict(set)
    lines = result.stdout.splitlines()
    i = 0
    current_files: dict[int, str] = {}

    while i < len(lines):
        stripped = lines[i].strip()
        if stripped.startswith("debug_line["):
            i, current_files = parse_dwarf_file_table(lines, i)
            continue
        if stripped.startswith("Address"):
            i += 2
            while i < len(lines):
                row = lines[i].split()
                if len(row) < 4 or not row[0].startswith("0x"):
                    break
                try:
                    lineno = int(row[1], 10)
                    file_index = int(row[3], 10)
                except ValueError:
                    i += 1
                    continue
                file_path = current_files.get(file_index)
                if lineno > 0 and file_path is not None:
                    compiled[file_path].add(lineno)
                i += 1
            continue
        i += 1

    return compiled


def collect_compiled_lines(elf: str, use_cache: bool = True) -> dict[str, set[int]]:
    cache_path = compiled_lines_cache_path(elf)
    if use_cache:
        try:
            with open(cache_path, "rb") as f:
                cached = pickle.load(f)
            print(f"  loaded compiled-line cache from {cache_path}")
            return {path: set(lines) for path, lines in cached.items()}
        except FileNotFoundError:
            pass
        except (OSError, pickle.PickleError, EOFError, AttributeError, ValueError):
            pass

    compiled = collect_compiled_lines_uncached(elf)
    if use_cache:
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            serializable = {path: sorted(lines) for path, lines in compiled.items()}
            with open(cache_path, "wb") as f:
                pickle.dump(serializable, f, protocol=pickle.HIGHEST_PROTOCOL)
            print(f"  wrote compiled-line cache to {cache_path}")
        except OSError:
            pass
    return compiled


def normalize_wos_source_path(path: str) -> str | None:
    resolved = Path(path).resolve(strict=False)
    if not is_wos_source_path(resolved):
        return None
    return str(resolved)


def load_sanitizer_ignorelist(path: Path = ROOT / "modules" / "kern" / "sancov.ignore") -> SanitizerIgnorelist:
    source_patterns: list[str] = []
    function_patterns: list[str] = []
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return SanitizerIgnorelist((), ())

    for raw_line in lines:
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        kind, separator, pattern = line.partition(":")
        if separator != ":" or not pattern:
            continue
        if kind == "src":
            source_patterns.append(pattern)
        elif kind == "fun":
            function_patterns.append(pattern)
    return SanitizerIgnorelist(tuple(source_patterns), tuple(function_patterns))


def ignorelist_matches_source(ignorelist: SanitizerIgnorelist, path: str) -> bool:
    rel = display_path(path)
    candidates = (path, rel)
    return any(fnmatch.fnmatch(candidate, pattern) for pattern in ignorelist.source_patterns for candidate in candidates)


def ignorelist_matches_function(ignorelist: SanitizerIgnorelist, name: str) -> bool:
    return any(fnmatch.fnmatch(name, pattern) for pattern in ignorelist.function_patterns)


def no_sanitize_reason(line: str) -> str | None:
    if "no_sanitize" not in line:
        return None
    lower = line.lower()
    reasons: list[str] = []
    if re.search(r'["\']coverage["\']', lower):
        reasons.append("no_sanitize(coverage)")
    if re.search(r'["\']kcfi["\']', lower):
        reasons.append("no_sanitize(kcfi)")
    if not reasons:
        return None
    return "+".join(reasons)


def find_attributed_range(lines: list[str], start_index: int) -> tuple[int, int]:
    saw_body = False
    depth = 0

    for index in range(start_index, len(lines)):
        line = lines[index]
        for offset, char in enumerate(line):
            if char == "{":
                saw_body = True
                depth += 1
            elif char == "}" and saw_body:
                depth -= 1
                if depth <= 0:
                    return start_index + 1, index + 1
            elif char == ";" and not saw_body:
                return start_index + 1, index + 1

        if saw_body and depth <= 0:
            return start_index + 1, index + 1

    return start_index + 1, start_index + 1


def collect_source_uncoverable_ranges(path: str) -> list[UncoverableRange]:
    try:
        lines = Path(path).read_text(errors="replace").splitlines()
    except OSError:
        return []

    ranges: list[UncoverableRange] = []
    for index, line in enumerate(lines):
        reason = no_sanitize_reason(line)
        if reason is None:
            continue
        start_line, end_line = find_attributed_range(lines, index)
        ranges.append(UncoverableRange(start_line, end_line, reason))
    return ranges


def add_uncoverable_line(
    lines_by_file: dict[str, dict[int, str]],
    path: str,
    line: int,
    reason: str,
) -> None:
    reasons = lines_by_file[path]
    if line in reasons and reason not in reasons[line].split("+"):
        reasons[line] = f"{reasons[line]}+{reason}"
    else:
        reasons.setdefault(line, reason)


def add_uncoverable_function(
    functions_by_file: dict[str, dict[str, str]],
    path: str,
    name: str,
    reason: str,
) -> None:
    reasons = functions_by_file[path]
    if name in reasons and reason not in reasons[name].split("+"):
        reasons[name] = f"{reasons[name]}+{reason}"
    else:
        reasons.setdefault(name, reason)


def record_overlaps_uncoverable_lines(record: FunctionRecord, uncoverable_lines: dict[int, str]) -> str | None:
    end_line = record.end_line if record.end_line is not None else record.line
    reasons: list[str] = []
    for line in range(record.line, end_line + 1):
        reason = uncoverable_lines.get(line)
        if reason is not None and reason not in reasons:
            reasons.append(reason)
    return "+".join(reasons) if reasons else None


def collect_coverage_exclusions(
    compiled_lines: dict[str, set[int]],
    functions_by_file: dict[str, dict[str, FunctionRecord]],
    ignorelist: SanitizerIgnorelist | None = None,
) -> CoverageExclusions:
    ignorelist = ignorelist if ignorelist is not None else load_sanitizer_ignorelist()
    lines_by_file: dict[str, dict[int, str]] = defaultdict(dict)
    functions_by_file_out: dict[str, dict[str, str]] = defaultdict(dict)

    for path, lines in compiled_lines.items():
        if Path(path).suffix in UNCOVERABLE_SOURCE_SUFFIXES:
            for line in lines:
                add_uncoverable_line(lines_by_file, path, line, "assembly-no-kcov")
            continue

        if ignorelist_matches_source(ignorelist, path):
            for line in lines:
                add_uncoverable_line(lines_by_file, path, line, "sanitizer-ignorelist(src)")
            continue

        ranges = collect_source_uncoverable_ranges(path)
        for source_range in ranges:
            for line in lines:
                if source_range.start_line <= line <= source_range.end_line:
                    add_uncoverable_line(lines_by_file, path, line, source_range.reason)

    for path, records in functions_by_file.items():
        file_uncoverable = lines_by_file.get(path, {})
        whole_file_reason = None
        if compiled_lines.get(path) and len(file_uncoverable) == len(compiled_lines[path]):
            whole_file_reason = next(iter(file_uncoverable.values()), "uncoverable")

        for name, record in records.items():
            reason = whole_file_reason
            if reason is None and ignorelist_matches_function(ignorelist, name):
                reason = "sanitizer-ignorelist(fun)"
            if reason is None:
                reason = record_overlaps_uncoverable_lines(record, file_uncoverable)
            if reason is not None:
                add_uncoverable_function(functions_by_file_out, path, name, reason)

    return CoverageExclusions(dict(lines_by_file), dict(functions_by_file_out))


def apply_line_exclusions(
    lines_by_file: dict[str, set[int]],
    exclusions: CoverageExclusions,
) -> dict[str, set[int]]:
    return {
        path: set(lines) - set(exclusions.lines_by_file.get(path, {}))
        for path, lines in lines_by_file.items()
    }


def apply_function_exclusions(
    functions_by_file: dict[str, dict[str, FunctionRecord]],
    exclusions: CoverageExclusions,
) -> dict[str, dict[str, FunctionRecord]]:
    result: dict[str, dict[str, FunctionRecord]] = {}
    for path, functions in functions_by_file.items():
        excluded = exclusions.functions_by_file.get(path, {})
        result[path] = {
            name: record
            for name, record in functions.items()
            if name not in excluded
        }
    return result


def filter_uncoverable_locations(
    locations: list[SymbolizedLocation],
    exclusions: CoverageExclusions,
    ignorelist: SanitizerIgnorelist | None = None,
) -> list[SymbolizedLocation]:
    ignorelist = ignorelist if ignorelist is not None else load_sanitizer_ignorelist()
    filtered: list[SymbolizedLocation] = []
    for location in locations:
        path = normalize_wos_source_path(location.file)
        if path is None:
            continue
        if location.line in exclusions.lines_by_file.get(path, {}):
            continue
        if location.function in exclusions.functions_by_file.get(path, {}):
            continue
        if ignorelist_matches_function(ignorelist, location.function):
            continue
        filtered.append(location)
    return filtered


def exclusion_reason_summary(exclusions: CoverageExclusions) -> Counter[str]:
    reasons: Counter[str] = Counter()
    for line_reasons in exclusions.lines_by_file.values():
        for reason in line_reasons.values():
            reasons.update(reason.split("+"))
    for function_reasons in exclusions.functions_by_file.values():
        for reason in function_reasons.values():
            reasons.update(reason.split("+"))
    return reasons


def collect_text_symbols(elf: str) -> list[TextSymbol]:
    result = subprocess.run(
        ["llvm-nm", "-P", "-S", "--defined-only", elf],
        capture_output=True,
        text=True,
        check=True,
    )
    symbols: list[TextSymbol] = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        name, symbol_type, address_text = parts[:3]
        if symbol_type not in ("T", "t", "W", "w"):
            continue
        if name.startswith(".L") or not is_reportable_function_name(name):
            continue
        try:
            address = int(address_text, 16)
            size = int(parts[3], 16) if len(parts) > 3 else 0
        except ValueError:
            continue
        symbols.append(TextSymbol(name, address, size))
    return symbols


def collect_function_definitions(elf: str) -> dict[str, dict[str, FunctionRecord]]:
    symbols = collect_text_symbols(elf)
    symbol_addresses: set[int] = set()
    for symbol in symbols:
        symbol_addresses.add(symbol.address)
        if symbol.size > 0:
            symbol_addresses.add(symbol.address + symbol.size - 1)
    locations_by_address = symbolize_address_locations(sorted(symbol_addresses), elf)
    functions: dict[str, dict[str, FunctionRecord]] = defaultdict(dict)

    for symbol in symbols:
        locations = locations_by_address.get(symbol.address, [])
        if not locations:
            continue
        location = locations[0]
        path = normalize_wos_source_path(location.file)
        if path is None:
            continue
        name = location.function if is_reportable_function_name(location.function) else symbol.name
        if not is_reportable_function_name(name):
            continue
        end_line = location.line
        if symbol.size > 0:
            end_address = symbol.address + symbol.size - 1
            for end_location in locations_by_address.get(end_address, []):
                end_path = normalize_wos_source_path(end_location.file)
                if end_path == path and is_reportable_function_name(end_location.function):
                    end_line = max(end_line, end_location.line)
                    break
        existing = functions[path].get(name)
        if existing is None or location.line < existing.line:
            functions[path][name] = FunctionRecord(location.line, name, end_line=end_line)
        elif existing.end_line is None or end_line > existing.end_line:
            functions[path][name] = FunctionRecord(existing.line, existing.name, existing.hit_count, end_line)

    return functions


def build_function_coverage(
    hit_locations: list[SymbolizedLocation],
    function_definitions: dict[str, dict[str, FunctionRecord]],
    line_coverage: dict[str, dict[int, int]] | None = None,
) -> dict[str, dict[str, FunctionRecord]]:
    coverage: dict[str, dict[str, FunctionRecord]] = {
        path: dict(records)
        for path, records in function_definitions.items()
    }

    for location in hit_locations:
        path = normalize_wos_source_path(location.file)
        if path is None or not is_reportable_function_name(location.function):
            continue
        if line_coverage is not None and line_coverage.get(path, {}).get(location.line, 0) <= 0:
            continue

        records = coverage.setdefault(path, {})
        existing = records.get(location.function)
        if existing is None:
            records[location.function] = FunctionRecord(location.line, location.function, 1, location.line)
            continue

        records[location.function] = FunctionRecord(
            min(existing.line, location.line),
            existing.name,
            existing.hit_count + 1,
            max(existing.end_line if existing.end_line is not None else existing.line, location.line),
        )

    return coverage


def build_coverage_map(hit_lines: dict[str, set[int]], compiled_lines: dict[str, set[int]]) -> dict[str, dict[int, int]]:
    coverage: dict[str, dict[int, int]] = {}

    for path, lines in compiled_lines.items():
        if not lines:
            continue
        coverage[path] = {line: 0 for line in sorted(lines)}

    for path, lines in hit_lines.items():
        line_hits = coverage.get(path)
        if line_hits is None:
            continue
        for line in sorted(lines):
            if line in line_hits:
                line_hits[line] = 1

    return coverage


def unmapped_hit_lines(hit_lines: dict[str, set[int]], compiled_lines: dict[str, set[int]]) -> dict[str, set[int]]:
    unmapped: dict[str, set[int]] = {}
    for path, lines in hit_lines.items():
        missing = lines - compiled_lines.get(path, set())
        if missing:
            unmapped[path] = missing
    return unmapped


def display_path(path: str, src_filter: str = "") -> str:
    if src_filter and src_filter in path:
        return path[path.index(src_filter):]
    try:
        return str(Path(path).resolve(strict=False).relative_to(ROOT))
    except ValueError:
        return path


def print_unmapped_hit_warning(unmapped: dict[str, set[int]], src_filter: str, limit: int = 10) -> None:
    filtered = {
        path: lines
        for path, lines in unmapped.items()
        if not src_filter or src_filter in path
    }
    if not filtered:
        return

    line_count = sum(len(lines) for lines in filtered.values())
    line_word = "line" if line_count == 1 else "lines"
    print(
        f"WARNING: ignored {line_count} symbolized hit {line_word} across {len(filtered)} files "
        "because they are not present in the compiled-line universe.",
        file=sys.stderr,
    )
    shown = 0
    for path in sorted(filtered):
        if shown >= limit:
            break
        sample = ", ".join(str(line) for line in sorted(filtered[path])[:8])
        print(f"  - {display_path(path, src_filter)}: {sample}", file=sys.stderr)
        shown += 1
    if len(filtered) > shown:
        print(f"  ... {len(filtered) - shown} more files omitted", file=sys.stderr)


def unmapped_hit_failures(unmapped: dict[str, set[int]], src_filter: str) -> list[str]:
    filtered = {
        path: lines
        for path, lines in unmapped.items()
        if not src_filter or src_filter in path
    }
    if not filtered:
        return []

    line_count = sum(len(lines) for lines in filtered.values())
    return [
        f"{line_count} symbolized hit line(s) across {len(filtered)} file(s) are not present "
        "in the compiled-line universe; refusing strict coverage"
    ]


def filtered_coverage(by_file: dict[str, dict[int, int]], src_filter: str) -> dict[str, dict[int, int]]:
    return {f: lines for f, lines in by_file.items() if not src_filter or src_filter in f}


def coverage_summary(coverage: dict[str, dict[int, int]]) -> CoverageSummary:
    return CoverageSummary(
        files=len(coverage),
        hit_lines=sum(sum(v.values()) for v in coverage.values()),
        total_lines=sum(len(v) for v in coverage.values()),
    )


def threshold_failures(
    by_file: dict[str, dict[int, int]],
    src_filter: str,
    fail_under_lines: float | None,
    fail_under_file_lines: float | None,
) -> list[str]:
    if fail_under_lines is None and fail_under_file_lines is None:
        return []

    filtered = filtered_coverage(by_file, src_filter)
    if not filtered:
        return ["No coverage data matched (check --src filter)."]

    failures: list[str] = []
    summary = coverage_summary(filtered)
    if fail_under_lines is not None and summary.percent + 1e-9 < fail_under_lines:
        failures.append(
            f"total line coverage {summary.percent:.1f}% is below required {fail_under_lines:.1f}% "
            f"({summary.hit_lines}/{summary.total_lines})"
        )

    if fail_under_file_lines is not None:
        for path in sorted(filtered):
            line_count = len(filtered[path])
            hit_count = sum(filtered[path].values())
            coverage_pct = (100.0 * hit_count / line_count) if line_count else 0.0
            if coverage_pct + 1e-9 < fail_under_file_lines:
                failures.append(
                    f"{path}: line coverage {coverage_pct:.1f}% is below required {fail_under_file_lines:.1f}% "
                    f"({hit_count}/{line_count})"
                )
    return failures


def print_report(
    by_file: dict[str, dict[int, int]],
    src_filter: str,
    uncoverable_lines_by_file: dict[str, dict[int, str]] | None = None,
) -> CoverageSummary | None:
    uncoverable_lines_by_file = uncoverable_lines_by_file or {}
    filtered = filtered_coverage(by_file, src_filter)
    filtered_uncoverable = {
        path: lines
        for path, lines in uncoverable_lines_by_file.items()
        if not src_filter or src_filter in path
    }
    paths = sorted(set(filtered) | set(filtered_uncoverable))
    if not paths:
        print("No coverage data matched (check --src filter).")
        return None

    summary = coverage_summary(filtered)
    uncoverable_total = sum(len(lines) for lines in filtered_uncoverable.values())
    col = min(max((len(f) for f in paths), default=4), 80)
    has_uncoverable = uncoverable_total > 0

    if has_uncoverable:
        print(f"{'File':<{col}}  Hit/Total  Uncoverable  Cover")
        print("-" * (col + 39))
    else:
        print(f"{'File':<{col}}  Hit/Total  Cover")
        print("-" * (col + 24))
    for path in paths:
        display = display_path(path, src_filter)
        hit_count = sum(filtered.get(path, {}).values())
        total_count = len(filtered.get(path, {}))
        uncoverable_count = len(filtered_uncoverable.get(path, {}))
        coverage_pct = (100.0 * hit_count / total_count) if total_count else 0.0
        cover_text = f"{coverage_pct:5.1f}%" if total_count else "  n/a"
        if has_uncoverable:
            print(f"{display:<{col}}  {hit_count:>3}/{total_count:<5}  {uncoverable_count:>11}  {cover_text}")
        else:
            print(f"{display:<{col}}  {hit_count:>3}/{total_count:<5}  {cover_text}")
    print("-" * (col + (39 if has_uncoverable else 24)))
    print(
        f"Total: {summary.hit_lines}/{summary.total_lines} coverable unique lines hit "
        f"across {summary.files} files ({summary.percent:.1f}%)"
    )
    if has_uncoverable:
        print(f"Uncoverable: {uncoverable_total} lines across {len(filtered_uncoverable)} files")
    return summary


def write_lcov(
    by_file: dict[str, dict[int, int]],
    out_path: str,
    functions_by_file: dict[str, dict[str, FunctionRecord]] | None = None,
) -> None:
    with open(out_path, "w") as f:
        for path in sorted(by_file):
            total_lines = len(by_file[path])
            hit_lines = sum(by_file[path].values())
            functions = functions_by_file.get(path, {}) if functions_by_file is not None else {}
            function_records = normalize_lcov_function_records(functions, by_file[path])
            f.write(f"SF:{path}\n")
            for record in function_records:
                f.write(f"FN:{record.line},{record.end_line},{record.name}\n")
            for record in function_records:
                f.write(f"FNDA:{record.hit_count},{record.name}\n")
            if function_records:
                f.write(f"FNF:{len(function_records)}\n")
                f.write(f"FNH:{sum(1 for record in function_records if record.hit_count > 0)}\n")
            for line, hit_count in sorted(by_file[path].items()):
                f.write(f"DA:{line},{hit_count}\n")
            f.write(f"LF:{total_lines}\n")
            f.write(f"LH:{hit_lines}\n")
            f.write("end_of_record\n")
    print(f"\nlcov written to {out_path}")
    print(f"  {genhtml_command(out_path)}")


def normalize_lcov_function_records(
    functions: dict[str, FunctionRecord],
    line_coverage: dict[int, int] | None = None,
) -> list[FunctionRecord]:
    end_line_by_start: dict[int, int] = {}
    for record in functions.values():
        end_line = record.end_line if record.end_line is not None else record.line
        end_line_by_start[record.line] = max(end_line_by_start.get(record.line, record.line), end_line)

    records: list[FunctionRecord] = []
    for record in functions.values():
        end_line = end_line_by_start[record.line]
        hit_count = record.hit_count
        if hit_count <= 0 and line_coverage is not None:
            for line, line_hit_count in line_coverage.items():
                if record.line <= line <= end_line and line_hit_count > 0:
                    hit_count = 1
                    break
        records.append(FunctionRecord(record.line, record.name, hit_count, end_line))
    return sorted(records, key=lambda item: (item.line, item.name))


def function_coverage_summary(
    functions_by_file: dict[str, dict[str, FunctionRecord]],
    line_coverage: dict[str, dict[int, int]],
) -> tuple[int, int, int]:
    total = 0
    hit = 0
    files = 0
    for path in sorted(line_coverage):
        records = normalize_lcov_function_records(functions_by_file.get(path, {}), line_coverage[path])
        if not records:
            continue
        files += 1
        total += len(records)
        hit += sum(1 for record in records if record.hit_count > 0)
    return hit, total, files


def genhtml_command(out_path: str) -> str:
    return f"genhtml {shlex.quote(out_path)} -o coverage-html/ && xdg-open coverage-html/index.html"


def coverage_threshold_arg(text: str) -> float:
    try:
        value = float(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{text!r} is not a number") from exc
    if value < 0.0 or value > 100.0:
        raise argparse.ArgumentTypeError("coverage threshold must be between 0 and 100")
    return value


def main() -> None:
    ap = argparse.ArgumentParser(description="Symbolize ktest KCOV coverage dump")
    ap.add_argument("-e", "--elf", default="build-ktest/modules/kern/wos")
    ap.add_argument("-l", "--log", default="ktest-data/serial-vm0.log")
    ap.add_argument("--lcov", metavar="FILE", help="Write lcov .info file")
    ap.add_argument("--no-cache", action="store_true", help="Bypass the compiled-line cache for this run")
    ap.add_argument(
        "--allow-elf-mismatch",
        action="store_true",
        help="Continue even when the log fingerprint does not match --elf",
    )
    ap.add_argument(
        "--allow-missing-fingerprint",
        action="store_true",
        help="Continue strict coverage checks even when the log has no KCOV ELF fingerprint",
    )
    ap.add_argument(
        "--allow-truncated",
        action="store_true",
        help="Continue even when the KCOV selftest buffer reported truncation",
    )
    ap.add_argument(
        "--allow-unmapped-hits",
        action="store_true",
        help="Continue strict coverage checks even when symbolized hits are missing from the compiled-line universe",
    )
    ap.add_argument("--src", metavar="PATH", default="", help="Filter to files containing PATH")
    ap.add_argument(
        "--fail-under-lines",
        metavar="PCT",
        type=coverage_threshold_arg,
        help="Exit non-zero if total filtered line coverage is below PCT",
    )
    ap.add_argument(
        "--fail-under-file-lines",
        metavar="PCT",
        type=coverage_threshold_arg,
        help="Exit non-zero if any filtered file's line coverage is below PCT",
    )
    ap.add_argument(
        "--require-100",
        action="store_true",
        help="Require 100%% total and per-file line coverage for the selected source set",
    )
    args = ap.parse_args()

    if args.require_100:
        args.fail_under_lines = 100.0
        args.fail_under_file_lines = 100.0
    strict_thresholds = args.fail_under_lines is not None or args.fail_under_file_lines is not None

    print(f"Reading PCs from {args.log} ...")
    try:
        report = extract_latest_kcov_report(args.log)
    except CoverageLogError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
    pcs = report.pcs
    if not pcs:
        print("No kcov block found. Build with WOS_KCOV=ON + WOS_SELFTEST=ON and boot with --selftest.")
        sys.exit(1)

    unique_pcs = list(set(pcs))
    print(f"  {len(pcs)} total PCs, {len(unique_pcs)} unique")
    validate_log_elf_match(
        report.fingerprint,
        args.elf,
        args.allow_elf_mismatch,
        require_fingerprint=strict_thresholds and not args.allow_missing_fingerprint,
    )
    validate_not_truncated(report.truncated, args.allow_truncated)
    validate_declared_count(report)

    print(f"Symbolizing against {args.elf} ...")
    locs = symbolize_locations(unique_pcs, args.elf)
    hit_lines: dict[str, set[int]] = defaultdict(set)
    for location in locs:
        path = normalize_wos_source_path(location.file)
        if path is not None:
            hit_lines[path].add(location.line)

    print(f"Collecting compiled source lines from {args.elf} ...")
    compiled_lines = collect_compiled_lines(args.elf, use_cache=not args.no_cache)
    function_definitions = collect_function_definitions(args.elf)

    print("Collecting coverage exclusions ...")
    ignorelist = load_sanitizer_ignorelist()
    exclusions = collect_coverage_exclusions(compiled_lines, function_definitions, ignorelist)
    coverable_compiled_lines = apply_line_exclusions(compiled_lines, exclusions)
    coverable_hit_locations = filter_uncoverable_locations(locs, exclusions, ignorelist)
    coverable_hit_lines: dict[str, set[int]] = defaultdict(set)
    for location in coverable_hit_locations:
        path = normalize_wos_source_path(location.file)
        if path is not None:
            coverable_hit_lines[path].add(location.line)
    by_file = build_coverage_map(coverable_hit_lines, coverable_compiled_lines)
    unmapped = unmapped_hit_lines(coverable_hit_lines, coverable_compiled_lines)
    reason_summary = exclusion_reason_summary(exclusions)
    reason_text = ", ".join(f"{reason}={count}" for reason, count in sorted(reason_summary.items()))
    print(
        f"  {exclusions.line_count} uncoverable lines and {exclusions.function_count} uncoverable functions "
        f"across {len(exclusions.lines_by_file)} files"
    )
    if reason_text:
        print(f"  reasons: {reason_text}")
    print(f"  {sum(len(v) for v in coverable_hit_lines.values())} unique coverable hit lines across {len(coverable_hit_lines)} files")
    print(f"  {sum(len(v) for v in coverable_compiled_lines.values())} coverable compiled lines across {len(coverable_compiled_lines)} files\n")

    print(f"Collecting function coverage from {args.elf} ...")
    coverable_function_definitions = apply_function_exclusions(function_definitions, exclusions)
    function_coverage = build_function_coverage(coverable_hit_locations, coverable_function_definitions, by_file)
    function_hit, function_total, function_files = function_coverage_summary(function_coverage, by_file)
    print(f"  {function_hit}/{function_total} functions hit across {function_files} files\n")

    print_report(by_file, args.src, exclusions.lines_by_file)
    print_unmapped_hit_warning(unmapped, args.src)

    failures: list[str] = []
    if strict_thresholds and not args.allow_unmapped_hits:
        failures.extend(unmapped_hit_failures(unmapped, args.src))
    failures.extend(threshold_failures(by_file, args.src, args.fail_under_lines, args.fail_under_file_lines))
    if failures:
        print("\nCoverage threshold failure:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        sys.exit(3)

    if args.lcov:
        write_lcov(by_file, args.lcov, function_coverage)


if __name__ == "__main__":
    main()
