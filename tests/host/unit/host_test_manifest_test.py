#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
HOST_DIR = ROOT / "tests" / "host"
HOST_CMAKE = HOST_DIR / "CMakeLists.txt"
ROOT_CMAKE = ROOT / "tests" / "CMakeLists.txt"
RUN_TESTS = ROOT / "scripts" / "test" / "run_tests.sh"
GTEST_MACRO_RE = re.compile(r"\b(?:TEST|TEST_F|TEST_P|TYPED_TEST|TYPED_TEST_P)\s*\(")
FUZZ_ENTRYPOINT_RE = re.compile(r"\bLLVMFuzzerTestOneInput\s*\(")
PYTHON_TEST_RE = re.compile(r"^\s*def\s+test_[A-Za-z0-9_]+\s*\(", flags=re.MULTILINE)


def fail(message: str) -> None:
    raise AssertionError(message)


def strip_comments(source: str) -> str:
    return "\n".join(line.split("#", 1)[0] for line in source.splitlines())


def registered_calls(source: str, function_name: str) -> dict[str, str]:
    calls: dict[str, str] = {}
    for match in re.finditer(rf"\b{function_name}\(\s*([A-Za-z_][A-Za-z0-9_]*)\b(?P<body>.*?)\)", source, flags=re.DOTALL):
        calls[match.group(1)] = match.group("body")
    return calls


def registered_python_tests(source: str) -> dict[str, str]:
    tests: dict[str, str] = {}
    for match in re.finditer(r"\badd_test\((?P<body>.*?)\)", source, flags=re.DOTALL):
        body = match.group("body")
        name_match = re.search(r"\bNAME\s+([A-Za-z_][A-Za-z0-9_]*)", body)
        command_match = re.search(r"\$\{CMAKE_CURRENT_SOURCE_DIR\}/unit/([A-Za-z0-9_]+_test\.py)", body)
        if name_match is not None and command_match is not None:
            tests[name_match.group(1)] = command_match.group(1)
    return tests


def registered_raw_ctest_names(source: str) -> set[str]:
    names: set[str] = set()
    for match in re.finditer(r"\badd_test\((?P<body>.*?)\)", source, flags=re.DOTALL):
        body = match.group("body")
        name_match = re.search(r"\bNAME\s+([A-Za-z_][A-Za-z0-9_]*)", body)
        if name_match is not None:
            names.add(name_match.group(1))
    return names


def registered_test_labels(source: str) -> dict[str, set[str]]:
    labels: dict[str, set[str]] = {}
    for match in re.finditer(r"\bset_tests_properties\((?P<body>.*?)\)", source, flags=re.DOTALL):
        body = match.group("body")
        properties_index = body.find("PROPERTIES")
        if properties_index < 0:
            continue
        label_match = re.search(r'\bLABELS\s+"([^"]+)"', body[properties_index:])
        if label_match is None:
            continue
        test_names = re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", body[:properties_index])
        for test_name in test_names:
            labels.setdefault(test_name, set()).update(label_match.group(1).split(";"))
    return labels


def require_registered_files(
    files: list[Path],
    registrations: dict[str, str],
    source_dir: str,
    registration_kind: str,
) -> None:
    missing = []
    wrong_source = []
    for path in files:
        target = path.stem
        body = registrations.get(target)
        if body is None:
            missing.append(str(path.relative_to(ROOT)))
            continue
        expected_source = f"${{CMAKE_CURRENT_SOURCE_DIR}}/{source_dir}/{path.name}"
        if expected_source not in body:
            wrong_source.append(f"{target} missing source {expected_source}")
    if missing:
        fail(f"{registration_kind} files not registered in tests/host/CMakeLists.txt: {', '.join(missing)}")
    if wrong_source:
        fail(f"{registration_kind} registrations with stale/missing source paths: {'; '.join(wrong_source)}")


def require_no_stale_sources(source: str, existing: set[str]) -> None:
    referenced = set(re.findall(r"\$\{CMAKE_CURRENT_SOURCE_DIR\}/((?:unit|fuzz)/[A-Za-z0-9_]+\.(?:cpp|py))", source))
    stale = sorted(referenced - existing)
    if stale:
        fail(f"tests/host/CMakeLists.txt references missing test source files: {', '.join(stale)}")


def mask_cxx_non_code(source: str) -> str:
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
                    out.extend(" " for _ in terminator)
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


def require_registered_test_sources_define_entrypoints(unit_cpp_files: list[Path], fuzz_cpp_files: list[Path]) -> None:
    empty_unit_tests = []
    for path in unit_cpp_files:
        source = mask_cxx_non_code(path.read_text())
        if GTEST_MACRO_RE.search(source) is None:
            empty_unit_tests.append(str(path.relative_to(ROOT)))
    if empty_unit_tests:
        fail("host C++ unit test files must define at least one real GTest case: " + ", ".join(empty_unit_tests))

    empty_fuzz_targets = []
    for path in fuzz_cpp_files:
        source = mask_cxx_non_code(path.read_text())
        if FUZZ_ENTRYPOINT_RE.search(source) is None:
            empty_fuzz_targets.append(str(path.relative_to(ROOT)))
    if empty_fuzz_targets:
        fail("host fuzz target files must define LLVMFuzzerTestOneInput: " + ", ".join(empty_fuzz_targets))


def require_discoverable_test_source_names() -> None:
    nested_test_sources = sorted(
        path
        for root in (HOST_DIR / "unit", HOST_DIR / "fuzz")
        for path in root.rglob("*")
        if path.is_file() and path.parent != root and path.suffix in {".cpp", ".py"}
    )
    if nested_test_sources:
        fail(
            "host test sources must stay flat under tests/host/unit or tests/host/fuzz so manifest scans cannot miss them: "
            + ", ".join(str(path.relative_to(ROOT)) for path in nested_test_sources)
        )

    hidden_unit_tests = []
    for path in sorted((HOST_DIR / "unit").glob("*.cpp")):
        source = mask_cxx_non_code(path.read_text())
        if GTEST_MACRO_RE.search(source) is not None and not path.name.endswith("_test.cpp"):
            hidden_unit_tests.append(str(path.relative_to(ROOT)))
    if hidden_unit_tests:
        fail("host GTest sources must use *_test.cpp names so manifest scans cannot miss them: " + ", ".join(hidden_unit_tests))

    hidden_python_tests = []
    for path in sorted((HOST_DIR / "unit").glob("*.py")):
        source = path.read_text()
        if PYTHON_TEST_RE.search(source) is not None and not path.name.endswith("_test.py"):
            hidden_python_tests.append(str(path.relative_to(ROOT)))
    if hidden_python_tests:
        fail("host Python tests must use *_test.py names so manifest scans cannot miss them: " + ", ".join(hidden_python_tests))

    hidden_fuzz_targets = []
    for path in sorted((HOST_DIR / "fuzz").glob("*.cpp")):
        source = mask_cxx_non_code(path.read_text())
        if FUZZ_ENTRYPOINT_RE.search(source) is not None and not path.name.endswith("_fuzz.cpp"):
            hidden_fuzz_targets.append(str(path.relative_to(ROOT)))
    if hidden_fuzz_targets:
        fail("host fuzz sources must use *_fuzz.cpp names so manifest scans cannot miss them: " + ", ".join(hidden_fuzz_targets))


def require_fuzz_helper_smoke_test() -> None:
    source = strip_comments(ROOT_CMAKE.read_text())
    match = re.search(r"\bfunction\(wos_add_fuzz_target\s+name\)(?P<body>.*?)\bendfunction\(\)", source, flags=re.DOTALL)
    if match is None:
        fail("tests/CMakeLists.txt is missing the wos_add_fuzz_target helper")

    body = match.group("body")
    add_test_index = body.find("add_test(")
    early_return_index = body.find("return()")
    if early_return_index >= 0 and (add_test_index < 0 or early_return_index < add_test_index):
        fail("wos_add_fuzz_target can return before registering its CTest smoke run")
    required_tokens = [
        "add_test(",
        "NAME ${name}_smoke",
        "COMMAND ${name}",
        "-runs=${WOS_HOST_FUZZ_SMOKE_RUNS}",
        "-max_len=${WOS_HOST_FUZZ_SMOKE_MAX_LEN}",
        "set_tests_properties(${name}_smoke PROPERTIES",
        'LABELS "host;fuzz;smoke"',
        "TIMEOUT 20",
    ]
    missing = [token for token in required_tokens if token not in body]
    if missing:
        fail(f"wos_add_fuzz_target does not register bounded CTest smoke runs: missing {', '.join(missing)}")


def require_unit_helper_ctest_labels() -> None:
    source = strip_comments(ROOT_CMAKE.read_text())
    match = re.search(r"\bfunction\(wos_add_unit_test\s+name\)(?P<body>.*?)\bendfunction\(\)", source, flags=re.DOTALL)
    if match is None:
        fail("tests/CMakeLists.txt is missing the wos_add_unit_test helper")
    if 'LABELS "host;unit"' not in match.group("body"):
        fail("wos_add_unit_test must label CTest entries as host;unit")


def shell_array_values(source: str, name: str) -> set[str]:
    match = re.search(rf"^{name}=\((?P<body>.*?)\)", source, flags=re.MULTILINE | re.DOTALL)
    if match is None:
        fail(f"scripts/test/run_tests.sh is missing {name}")
    return set(re.findall(r"[A-Za-z_][A-Za-z0-9_]*", match.group("body")))


def require_run_tests_lists_current(unit_targets: set[str], fuzz_targets: set[str]) -> None:
    source = RUN_TESTS.read_text()
    listed_units = shell_array_values(source, "UNIT_TESTS")
    listed_fuzz = shell_array_values(source, "FUZZ_TARGETS")
    require_sets_equal(unit_targets, listed_units, "CMake unit tests", "scripts/test/run_tests.sh UNIT_TESTS")
    require_sets_equal(fuzz_targets, listed_fuzz, "CMake fuzz targets", "scripts/test/run_tests.sh FUZZ_TARGETS")


def require_python_tests_ctest_labels(source: str, py_tests: set[str]) -> None:
    labels = registered_test_labels(source)
    required = {"host", "source"}
    missing = sorted(test for test in py_tests if not required.issubset(labels.get(test, set())))
    if missing:
        fail("Python host tests must be labeled host;source in CTest: " + ", ".join(missing))


def require_raw_ctest_entries_are_registered_python_tests(source: str, py_tests: set[str]) -> None:
    raw_ctest_names = registered_raw_ctest_names(source)
    require_sets_equal(
        py_tests,
        raw_ctest_names,
        "Python host test registrations",
        "raw tests/host/CMakeLists.txt add_test entries",
    )


def require_sets_equal(left: set[str], right: set[str], left_name: str, right_name: str) -> None:
    missing = sorted(left - right)
    extra = sorted(right - left)
    if missing or extra:
        parts = []
        if missing:
            parts.append(f"missing from {right_name}: {', '.join(missing)}")
        if extra:
            parts.append(f"extra in {right_name}: {', '.join(extra)}")
        fail(f"{left_name}/{right_name} mismatch: {'; '.join(parts)}")


def main() -> None:
    source = strip_comments(HOST_CMAKE.read_text())

    require_discoverable_test_source_names()

    unit_cpp_files = sorted((HOST_DIR / "unit").glob("*_test.cpp"))
    unit_py_files = sorted((HOST_DIR / "unit").glob("*_test.py"))
    fuzz_cpp_files = sorted((HOST_DIR / "fuzz").glob("*_fuzz.cpp"))
    existing = {str(path.relative_to(HOST_DIR)) for path in unit_cpp_files + unit_py_files + fuzz_cpp_files}

    require_registered_test_sources_define_entrypoints(unit_cpp_files, fuzz_cpp_files)

    unit_regs = registered_calls(source, "wos_add_unit_test")
    fuzz_regs = registered_calls(source, "wos_add_fuzz_target")
    py_regs = registered_python_tests(source)

    require_registered_files(unit_cpp_files, unit_regs, "unit", "C++ host unit test")
    require_registered_files(fuzz_cpp_files, fuzz_regs, "fuzz", "host fuzz target")

    missing_py = []
    wrong_py = []
    for path in unit_py_files:
        target = path.stem
        registered_file = py_regs.get(target)
        if registered_file is None:
            missing_py.append(str(path.relative_to(ROOT)))
            continue
        if registered_file != path.name:
            wrong_py.append(f"{target} points at {registered_file}, expected {path.name}")
    if missing_py:
        fail(f"Python host tests not registered in tests/host/CMakeLists.txt: {', '.join(missing_py)}")
    if wrong_py:
        fail(f"Python host test registrations with stale source paths: {'; '.join(wrong_py)}")

    require_no_stale_sources(source, existing)
    require_fuzz_helper_smoke_test()
    require_unit_helper_ctest_labels()
    require_python_tests_ctest_labels(source, set(py_regs))
    require_raw_ctest_entries_are_registered_python_tests(source, set(py_regs))
    require_run_tests_lists_current(set(unit_regs) | set(py_regs), set(fuzz_regs))
    print(
        f"{len(unit_cpp_files)} C++ unit tests, {len(unit_py_files)} Python tests, "
        f"and {len(fuzz_cpp_files)} fuzz targets are registered"
    )


if __name__ == "__main__":
    main()
