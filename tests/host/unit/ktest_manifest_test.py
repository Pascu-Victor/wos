#!/usr/bin/env python3

import re
import tempfile
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
KTEST_DIR = ROOT / "modules" / "kern" / "src" / "test"
KTEST_MAIN = KTEST_DIR / "ktest.cpp"
KTEST_HEADER = KTEST_DIR / "ktest.hpp"
KTEST_CMAKE = ROOT / "modules" / "kern" / "CMakeLists.txt"
KTEST_LINKER = ROOT / "modules" / "kern" / "linker.ld"
KERNEL_SRC_DIR = ROOT / "modules" / "kern" / "src"
HOST_UNIT_DIR = ROOT / "tests" / "host" / "unit"

KTEST_DECL_RE = re.compile(
    r"\b(?P<kind>KTEST|KTEST_OFF)\(\s*(?P<suite>[A-Za-z_][A-Za-z0-9_]*)\s*,\s*"
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*\)\s*\{"
)
ASSERTION_RE = re.compile(r"\bK(?:EXPECT|REQUIRE)_(?:EQ|NE|TRUE|FALSE|NULL)\s*\(")
DISABLED_REASON_RE = re.compile(r"//\s*(?:Requires|Disabled|Reason|TODO|Needs)\b", flags=re.IGNORECASE)
HOST_COVERAGE_RE = re.compile(r"//\s*Host coverage:\s*(?P<tests>[A-Za-z_][A-Za-z0-9_.]*(?:\s*,\s*[A-Za-z_][A-Za-z0-9_.]*)*)")
GTEST_RE = re.compile(
    r"\bTEST(?:_F)?\(\s*(?P<suite>[A-Za-z_][A-Za-z0-9_]*)\s*,\s*(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*\)"
)
KTEST_SECTION_LITERAL_RE = re.compile(r"\.ktests\b")
CXX_SOURCE_SUFFIXES = {".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp", ".hxx"}
ASM_SOURCE_SUFFIXES = {".S", ".s", ".asm", ".nasm"}


@dataclass(frozen=True)
class KTestDecl:
    path: Path
    line: int
    kind: str
    suite: str
    name: str
    body: str
    active_body: str

    @property
    def key(self) -> str:
        return f"{self.suite}/{self.name}"


def fail(message: str) -> None:
    raise AssertionError(message)


def source_line(source: str, offset: int) -> int:
    return source.count("\n", 0, offset) + 1


def mask_cxx_non_code_preserve_offsets(source: str) -> str:
    out: list[str] = []
    index = 0
    in_string = False
    in_char = False
    escaped = False

    def push_masked(char: str) -> None:
        out.append("\n" if char == "\n" else " ")

    def is_digit_separator_quote(offset: int) -> bool:
        if offset == 0 or offset + 1 >= len(source):
            return False
        previous = source[offset - 1]
        following = source[offset + 1]
        if not (previous.isalnum() or previous == "_"):
            return False
        if not (following.isalnum() or following == "_"):
            return False

        token_start = offset - 1
        while token_start >= 0 and (source[token_start].isalnum() or source[token_start] == "_"):
            token_start -= 1
        token_before_quote = source[token_start + 1 : offset]
        if token_before_quote in {"L", "U", "u", "u8"}:
            return False
        return any(char.isdigit() for char in token_before_quote)

    while index < len(source):
        char = source[index]
        next_char = source[index + 1] if index + 1 < len(source) else ""

        if in_string or in_char:
            push_masked(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif in_string and char == '"':
                in_string = False
            elif in_char and char == "'":
                in_char = False
            index += 1
            continue

        if char == "R" and next_char == '"':
            out.extend("  ")
            index += 2
            delimiter_start = index
            while index < len(source) and source[index] != "(":
                push_masked(source[index])
                index += 1
            delimiter = source[delimiter_start:index]
            if index >= len(source):
                break
            push_masked(source[index])
            index += 1
            terminator = ")" + delimiter + '"'
            while index < len(source):
                if source.startswith(terminator, index):
                    for _ in terminator:
                        out.append(" ")
                    index += len(terminator)
                    break
                push_masked(source[index])
                index += 1
            continue
        if char == '"':
            in_string = True
            push_masked(char)
            index += 1
            continue
        if char == "'" and not is_digit_separator_quote(index):
            in_char = True
            push_masked(char)
            index += 1
            continue
        if char == "/" and next_char == "/":
            out.extend("  ")
            index += 2
            while index < len(source) and source[index] != "\n":
                push_masked(source[index])
                index += 1
            continue
        if char == "/" and next_char == "*":
            out.extend("  ")
            index += 2
            while index < len(source):
                if source[index] == "*" and index + 1 < len(source) and source[index + 1] == "/":
                    out.extend("  ")
                    index += 2
                    break
                push_masked(source[index])
                index += 1
            continue

        out.append(char)
        index += 1

    return "".join(out)


def strip_cxx_comments_preserve_offsets(source: str) -> str:
    out: list[str] = []
    index = 0
    in_string = False
    in_char = False
    escaped = False

    def push_masked(char: str) -> None:
        out.append("\n" if char == "\n" else " ")

    def is_digit_separator_quote(offset: int) -> bool:
        if offset == 0 or offset + 1 >= len(source):
            return False
        previous = source[offset - 1]
        following = source[offset + 1]
        if not (previous.isalnum() or previous == "_"):
            return False
        if not (following.isalnum() or following == "_"):
            return False

        token_start = offset - 1
        while token_start >= 0 and (source[token_start].isalnum() or source[token_start] == "_"):
            token_start -= 1
        token_before_quote = source[token_start + 1 : offset]
        if token_before_quote in {"L", "U", "u", "u8"}:
            return False
        return any(char.isdigit() for char in token_before_quote)

    while index < len(source):
        char = source[index]
        next_char = source[index + 1] if index + 1 < len(source) else ""

        if in_string or in_char:
            out.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif in_string and char == '"':
                in_string = False
            elif in_char and char == "'":
                in_char = False
            index += 1
            continue

        if char == "R" and next_char == '"':
            delimiter_start = index + 2
            delimiter_end = source.find("(", delimiter_start)
            if delimiter_end < 0:
                out.append(char)
                index += 1
                continue
            delimiter = source[delimiter_start:delimiter_end]
            terminator = ")" + delimiter + '"'
            terminator_start = source.find(terminator, delimiter_end + 1)
            if terminator_start < 0:
                out.append(source[index:])
                break
            terminator_end = terminator_start + len(terminator)
            out.append(source[index:terminator_end])
            index = terminator_end
            continue
        if char == '"':
            in_string = True
            out.append(char)
            index += 1
            continue
        if char == "'" and not is_digit_separator_quote(index):
            in_char = True
            out.append(char)
            index += 1
            continue
        if char == "/" and next_char == "/":
            out.extend("  ")
            index += 2
            while index < len(source) and source[index] != "\n":
                push_masked(source[index])
                index += 1
            continue
        if char == "/" and next_char == "*":
            out.extend("  ")
            index += 2
            while index < len(source):
                if source[index] == "*" and index + 1 < len(source) and source[index + 1] == "/":
                    out.extend("  ")
                    index += 2
                    break
                push_masked(source[index])
                index += 1
            continue

        out.append(char)
        index += 1

    return "".join(out)


def strip_asm_comments_preserve_offsets(source: str) -> str:
    out: list[str] = []
    for line in source.splitlines(keepends=True):
        comment_start = len(line)
        for marker in ("#", ";"):
            marker_start = line.find(marker)
            if marker_start >= 0:
                comment_start = min(comment_start, marker_start)
        out.append(line[:comment_start])
        for char in line[comment_start:]:
            out.append("\n" if char == "\n" else " ")
    return "".join(out)


def matching_close_brace(source: str, open_brace: int, path: Path) -> int:
    depth = 0
    for index in range(open_brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return index
    fail(f"{path}: KTEST body starting at byte {open_brace} is unterminated")


def parse_ktests(path: Path) -> list[KTestDecl]:
    source = path.read_text()
    active_source = mask_cxx_non_code_preserve_offsets(source)
    if re.search(r"\bKTEST_IMPL\s*\(", active_source):
        fail(f"{path.relative_to(ROOT)} uses raw KTEST_IMPL; use KTEST or KTEST_OFF so manifest checks can classify it")
    matches = list(KTEST_DECL_RE.finditer(active_source))
    declarations: list[KTestDecl] = []
    for match in matches:
        close_brace = matching_close_brace(active_source, match.end() - 1, path)
        declarations.append(
            KTestDecl(
                path=path,
                line=source_line(source, match.start()),
                kind=match.group("kind"),
                suite=match.group("suite"),
                name=match.group("name"),
                body=source[match.end() : close_brace],
                active_body=active_source[match.end() : close_brace],
            )
        )
    raw_macro_count = len(re.findall(r"\bKTEST(?:_OFF)?\s*\(", active_source))
    if raw_macro_count != len(declarations):
        fail(f"{path.relative_to(ROOT)} has KTEST-like macros the manifest parser did not understand")
    return declarations


def require_parser_uses_matched_test_bodies() -> None:
    with tempfile.TemporaryDirectory(prefix="wos-ktest-manifest-parser-") as tmp_dir:
        synthetic = Path(tmp_dir) / "synthetic_ktest.cpp"
        synthetic.write_text(
            """
KTEST(ParserBounds, Empty) {
    helper_with_assertion();
}

static void helper_with_assertion() {
    KEXPECT_TRUE(false);
}

KTEST(ParserBounds, Covered) {
    KEXPECT_TRUE(true);
}

KTEST_OFF(ParserBounds, Disabled) {
}
// Requires comment outside the disabled test body must not count.
// Host coverage: Bitmap.InitAllClear
""".lstrip()
        )
        declarations = parse_ktests(synthetic)
    by_key = {declaration.key: declaration for declaration in declarations}
    if ASSERTION_RE.search(by_key["ParserBounds/Empty"].active_body) is not None:
        fail("KTEST parser allowed one test body to borrow an assertion outside its matching braces")
    if ASSERTION_RE.search(by_key["ParserBounds/Covered"].active_body) is None:
        fail("KTEST parser failed to retain assertions inside a matched test body")
    if DISABLED_REASON_RE.search(by_key["ParserBounds/Disabled"].body) is not None:
        fail("KTEST parser allowed disabled reason comments outside the matched test body")


def visible_source_for_section_scan(path: Path, source: str) -> str | None:
    if path.suffix in CXX_SOURCE_SUFFIXES:
        return strip_cxx_comments_preserve_offsets(source)
    if path.suffix in ASM_SOURCE_SUFFIXES:
        return strip_asm_comments_preserve_offsets(source)
    return None


def require_section_scanner_handles_comments_and_strings() -> None:
    comment_only = """
// [[gnu::section(".ktests"), gnu::used]]
/* __attribute__((section(".ktests"))) */
""".lstrip()
    if KTEST_SECTION_LITERAL_RE.search(strip_cxx_comments_preserve_offsets(comment_only)) is not None:
        fail("KTEST section scanner treated comments as active code")
    if KTEST_SECTION_LITERAL_RE.search('[[gnu::section(".ktests"), gnu::used]] static int record;') is None:
        fail("KTEST section scanner missed an active C++ section attribute")
    if KTEST_SECTION_LITERAL_RE.search(strip_cxx_comments_preserve_offsets('asm(".section .ktests");')) is None:
        fail("KTEST section scanner missed an active inline-assembly section switch")
    if KTEST_SECTION_LITERAL_RE.search(strip_asm_comments_preserve_offsets('; .section .ktests\n')) is not None:
        fail("KTEST section scanner treated assembly comments as active code")
    if KTEST_SECTION_LITERAL_RE.search(strip_asm_comments_preserve_offsets(".section .ktests\n")) is None:
        fail("KTEST section scanner missed an active assembly section switch")


def require_ktest_section_records_only_from_macro() -> None:
    offenders: list[str] = []
    header_source = ""
    header_visible_source = ""
    header_matches: list[re.Match[str]] = []

    for path in sorted(KERNEL_SRC_DIR.rglob("*")):
        if not path.is_file():
            continue
        source = path.read_text(errors="replace")
        visible_source = visible_source_for_section_scan(path, source)
        if visible_source is None:
            continue
        matches = list(KTEST_SECTION_LITERAL_RE.finditer(visible_source))
        if path == KTEST_HEADER:
            header_source = source
            header_visible_source = visible_source
            header_matches = matches
            continue
        offenders.extend(f"{path.relative_to(ROOT)}:{source_line(source, match.start())}" for match in matches)

    if offenders:
        fail(
            "manual .ktests linker-section records are not allowed outside KTEST_IMPL: "
            + ", ".join(offenders)
        )
    if len(header_matches) != 1:
        fail(f"{KTEST_HEADER.relative_to(ROOT)} must contain exactly one .ktests record owned by KTEST_IMPL")

    macro_start = header_visible_source.find("#define KTEST_IMPL")
    macro_end = header_visible_source.find("#define KTEST(S", macro_start)
    if macro_start < 0 or macro_end < 0:
        fail(f"{KTEST_HEADER.relative_to(ROOT)} must keep KTEST_IMPL before the public KTEST wrappers")
    if not macro_start <= header_matches[0].start() < macro_end:
        line = source_line(header_source, header_matches[0].start())
        fail(f"{KTEST_HEADER.relative_to(ROOT)}:{line} has a .ktests record outside KTEST_IMPL")

    linker_source = KTEST_LINKER.read_text()
    required_tokens = [
        ".ktests :",
        "__start_ktests = .;",
        "KEEP(*(.ktests))",
        "__stop_ktests = .;",
    ]
    missing = [token for token in required_tokens if token not in linker_source]
    if missing:
        fail("kernel linker script is missing expected KTEST section tokens: " + ", ".join(missing))


def all_ktests() -> list[KTestDecl]:
    declarations: list[KTestDecl] = []
    empty_test_sources: list[str] = []
    ktest_sources = sorted(KTEST_DIR.rglob("*_ktest.cpp"))
    unexpected_sources = sorted(
        path
        for path in KTEST_DIR.rglob("*.cpp")
        if path.name != "ktest.cpp" and not path.name.endswith("_ktest.cpp")
    )
    if unexpected_sources:
        fail(
            "KTEST C++ sources must be the runner or use the *_ktest.cpp suffix: "
            + ", ".join(str(path.relative_to(ROOT)) for path in unexpected_sources)
        )
    for path in ktest_sources:
        parsed = parse_ktests(path)
        if not parsed:
            empty_test_sources.append(str(path.relative_to(ROOT)))
        declarations.extend(parsed)
    if empty_test_sources:
        fail(f"KTEST source files without KTEST declarations: {', '.join(empty_test_sources)}")
    return declarations


def require_unique_tests(declarations: list[KTestDecl]) -> None:
    seen: dict[str, KTestDecl] = {}
    duplicates: list[str] = []
    for declaration in declarations:
        previous = seen.get(declaration.key)
        if previous is not None:
            duplicates.append(
                f"{declaration.key} at {declaration.path.relative_to(ROOT)}:{declaration.line} "
                f"duplicates {previous.path.relative_to(ROOT)}:{previous.line}"
            )
            continue
        seen[declaration.key] = declaration
    if duplicates:
        fail("duplicate KTEST registrations: " + "; ".join(duplicates))


def require_enabled_tests_have_assertions(declarations: list[KTestDecl]) -> None:
    missing = [
        f"{declaration.key} at {declaration.path.relative_to(ROOT)}:{declaration.line}"
        for declaration in declarations
        if declaration.kind == "KTEST" and ASSERTION_RE.search(declaration.active_body) is None
    ]
    if missing:
        fail("enabled KTESTs without KEXPECT/KREQUIRE assertions: " + ", ".join(missing))


def require_disabled_tests_are_explained(declarations: list[KTestDecl]) -> None:
    unexplained = [
        f"{declaration.key} at {declaration.path.relative_to(ROOT)}:{declaration.line}"
        for declaration in declarations
        if declaration.kind == "KTEST_OFF" and DISABLED_REASON_RE.search(declaration.body) is None
    ]
    if unexplained:
        fail("disabled KTESTs without an inline reason: " + ", ".join(unexplained))


def parse_host_gtests() -> set[str]:
    tests = set()
    for path in sorted(HOST_UNIT_DIR.glob("*.cpp")):
        source = mask_cxx_non_code_preserve_offsets(path.read_text())
        for match in GTEST_RE.finditer(source):
            tests.add(f"{match.group('suite')}.{match.group('name')}")
    return tests


def function_body(source: str, name: str) -> str:
    start = source.find(f"void {name}(")
    if start < 0:
        fail(f"{name} function not found")
    brace = source.find("{", start)
    if brace < 0:
        fail(f"{name} function has no body")

    depth = 0
    for index in range(brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[brace + 1:index]
    fail(f"{name} function body is unterminated")


def require_disabled_tests_have_host_coverage(declarations: list[KTestDecl]) -> None:
    host_tests = parse_host_gtests()
    missing_annotations = []
    missing_host_tests = []
    for declaration in declarations:
        if declaration.kind != "KTEST_OFF":
            continue
        coverage = HOST_COVERAGE_RE.search(declaration.body)
        if coverage is None:
            missing_annotations.append(f"{declaration.key} at {declaration.path.relative_to(ROOT)}:{declaration.line}")
            continue
        for host_test in (part.strip() for part in coverage.group("tests").split(",")):
            if host_test not in host_tests:
                missing_host_tests.append(f"{declaration.key} references missing host test {host_test}")
    if missing_annotations:
        fail("disabled KTESTs without Host coverage annotations: " + ", ".join(missing_annotations))
    if missing_host_tests:
        fail("disabled KTEST host coverage references are stale: " + "; ".join(missing_host_tests))


def require_runner_uses_linker_section() -> None:
    source = function_body(KTEST_MAIN.read_text(), "run_all")
    required_tokens = [
        "__start_ktests",
        "__stop_ktests",
        "t->fn == nullptr || t->suite == nullptr || t->name == nullptr",
        "if (!t->enabled)",
        "PASS_BEFORE = g_pass",
        "FAIL_BEFORE = g_fail",
        "t->fn()",
        "g_pass == PASS_BEFORE && g_fail == FAIL_BEFORE",
        "no assertions executed",
        "g_fail++",
        "panic_handler(\"selftest: failures detected\")",
    ]
    missing = [token for token in required_tokens if token not in source]
    if missing:
        fail("KTEST runner is missing expected linker-section execution tokens: " + ", ".join(missing))


def require_selftest_build_toggle() -> None:
    source = KTEST_CMAKE.read_text()
    required_tokens = [
        "option(WOS_SELFTEST",
        "function(wos_kernel_glob_recurse output_var)",
        "file(GLOB_RECURSE WOS_KERNEL_GLOB_RECURSE_OUTPUT ${WOS_KERNEL_GLOB_OPTIONS} ${ARGN})",
        "if(NOT WOS_SELFTEST)",
        "wos_kernel_glob_recurse(KTEST_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/src/test/*.cpp)",
        "list(REMOVE_ITEM CXXFILES ${KTEST_SRCS})",
    ]
    missing = [token for token in required_tokens if token not in source]
    if missing:
        fail("kernel CMake selftest source gating is missing expected tokens: " + ", ".join(missing))


def main() -> None:
    require_parser_uses_matched_test_bodies()
    require_section_scanner_handles_comments_and_strings()
    require_ktest_section_records_only_from_macro()

    declarations = all_ktests()
    require_unique_tests(declarations)
    require_enabled_tests_have_assertions(declarations)
    require_disabled_tests_are_explained(declarations)
    require_disabled_tests_have_host_coverage(declarations)
    require_runner_uses_linker_section()
    require_selftest_build_toggle()

    enabled = sum(1 for declaration in declarations if declaration.kind == "KTEST")
    disabled = len(declarations) - enabled
    print(f"{enabled} enabled KTESTs and {disabled} disabled KTESTs have manifest coverage")


if __name__ == "__main__":
    main()
