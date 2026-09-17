#!/usr/bin/env python3
"""Reject new raw effective-UID/root comparisons during the policy migration."""

from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[2]
MANIFEST = Path("tests/security/credential_authority_v1.json")
# Schema v1 must retain every authority domain throughout migration. In
# particular, deleting pending rows must never satisfy --require-complete.
REQUIRED_OPERATIONS = frozenset({
    "credentials", "exec", "resources", "vfs-traversal", "vfs-open-create",
    "vfs-mutation", "vfs-metadata", "mount", "shm", "signals", "ptrace",
    "priority", "power", "init-control", "procfs", "devices-debug",
    "network-hostname", "wki-identity", "wki-resources", "init-accounts",
    "rootfs-ownership",
})
NON_CODE = re.compile(
    r'R"(?P<delimiter>[^ ()\\\t\r\n]{0,16})\(.*?\)(?P=delimiter)"'
    r'|//[^\n]*|/\*.*?\*/|"(?:\\.|[^"\\])*"|(?<![A-Za-z0-9_])\'(?:\\.|[^\'\\\n])*\'',
    re.DOTALL,
)
# Match complete integral tokens without mistaking part of a floating or
# nonzero literal for integer zero. Apostrophes are C++ digit separators.
ZERO = r"(?<![\w.'])(?:0[xX]0(?:'?0)*|0[bB]0(?:'?0)*|0(?:'?0)*)[uUlLzZ]*(?![\w.'])"
# Recognize common integral casts/constructions without pretending to resolve
# arbitrary aliases, macros, overloaded functions or user-defined conversions.
INTEGRAL_TYPE = (
    r"(?:(?:std::)?(?:u?int(?:8|16|32|64|max)_t|size_t|ptrdiff_t)|uid_t|gid_t|bool|"
    r"(?:unsigned|signed)(?:\s+(?:char|short(?:\s+int)?|int|long(?:\s+long)?(?:\s+int)?))?|"
    r"char|short(?:\s+int)?|int|long(?:\s+long)?(?:\s+int)?)"
)
ZERO_VALUE = rf"(?:{ZERO}|\bfalse\b)"
ZERO_EXPRESSION = (
    rf"(?:\bstatic_cast\s*<\s*{INTEGRAL_TYPE}\s*>\s*\(\s*{ZERO_VALUE}\s*\)"
    rf"|\b{INTEGRAL_TYPE}\s*\{{\s*{ZERO_VALUE}\s*\}}"
    rf"|\b{INTEGRAL_TYPE}\s*\(\s*{ZERO_VALUE}\s*\)"
    rf"|\(\s*{INTEGRAL_TYPE}\s*\)\s*{ZERO_VALUE}"
    rf"|{ZERO_VALUE})"
)
MEMBER = r"\b(?:[A-Za-z_]\w*(?:\s*\(\s*\))?\s*(?:->|\.)\s*)*(?:euid|effective_uid|effective)\b(?:\s*\(\s*\))?"
COMPARISON = re.compile(
    rf"{MEMBER}\s*\)*\s*(?:==|!=|<=|>|\bnot_eq\b)\s*\(*\s*{ZERO_EXPRESSION}"
    rf"|{ZERO_EXPRESSION}\s*\)*\s*(?:==|!=|>=|<|\bnot_eq\b)\s*\(*\s*{MEMBER}"
    rf"|(?:!|\bnot\b)\s*\(*\s*{MEMBER}"
)


def code_only(source: str) -> str:
    return NON_CODE.sub(lambda match: re.sub(r"[^\n]", " ", match[0]), source)


def root_checks(source: str) -> Counter[str]:
    if "euid" not in source and "effective" not in source:
        return Counter()
    return Counter(re.sub(r"\s+", "", match[0]) for match in COMPARISON.finditer(code_only(source)))


def unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate manifest key: {key}")
        result[key] = value
    return result


def audit(root: Path, manifest: dict, *, require_complete: bool = False) -> int:
    if manifest.get("schema_version") != 1:
        raise ValueError("unsupported authority manifest version")
    operations = manifest.get("operations")
    if not isinstance(operations, list) or not operations:
        raise ValueError("missing operation inventory")
    ids = set()
    for operation in operations:
        if not isinstance(operation, dict):
            raise ValueError("invalid operation contract")
        name = operation.get("id")
        if not isinstance(name, str) or not name or name in ids:
            raise ValueError("invalid or duplicate operation ID")
        ids.add(name)
        if operation.get("status") not in {"pending", "enforced"} or not operation.get("decision"):
            raise ValueError(f"invalid operation contract: {name}")
        paths = operation.get("sources")
        if not isinstance(paths, list) or not paths:
            raise ValueError(f"missing operation sources: {name}")
        for path in paths:
            if not isinstance(path, str) or Path(path).is_absolute() or ".." in Path(path).parts or not (root / path).is_file():
                raise ValueError(f"missing or unsafe operation source: {path}")
        if require_complete and operation["status"] != "enforced":
            raise ValueError(f"operation still pending: {name}")

    missing = REQUIRED_OPERATIONS - ids
    if missing:
        raise ValueError(f"missing required operations: {', '.join(sorted(missing))}")

    expected = {}
    for entry in manifest.get("legacy_root_checks", []):
        path = entry.get("path")
        if path in expected or not entry.get("reason") or entry.get("operation") not in ids:
            raise ValueError(f"invalid legacy exception: {path}")
        checks = entry.get("checks")
        if not isinstance(checks, dict) or not checks or any(type(count) is not int or count <= 0 for count in checks.values()):
            raise ValueError(f"invalid legacy check counts: {path}")
        expected[path] = Counter(checks)
    if require_complete and expected:
        raise ValueError("legacy root checks remain")

    actual = {}
    kernel = root / "modules/kern/src"
    for path in sorted(kernel.rglob("*")):
        if path.suffix not in {".cpp", ".hpp", ".c", ".h"}:
            continue
        relative = path.relative_to(kernel)
        # Security policy owns root semantics; selftest fixtures may assert them.
        if relative.parts[0] == "test" or relative.parts[:2] == ("platform", "security"):
            continue
        checks = root_checks(path.read_text())
        if checks:
            actual[str(path.relative_to(root))] = checks
    if actual != expected:
        new = {path: dict(checks - expected.get(path, Counter())) for path, checks in actual.items()}
        stale = {path: dict(checks - actual.get(path, Counter())) for path, checks in expected.items()}
        raise ValueError(f"raw root-check drift: new={ {p: c for p, c in new.items() if c} }; stale={ {p: c for p, c in stale.items() if c} }")
    return sum(sum(checks.values()) for checks in actual.values())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--manifest", type=Path, default=MANIFEST)
    parser.add_argument("--require-complete", action="store_true")
    args = parser.parse_args()
    path = args.manifest if args.manifest.is_absolute() else args.root / args.manifest
    try:
        manifest = json.loads(path.read_text(), object_pairs_hook=unique_object)
        count = audit(args.root, manifest, require_complete=args.require_complete)
    except (ValueError, OSError) as error:
        print(f"credential policy audit failed: {error}")
        return 1
    print(f"credential policy audit passed; {count} documented legacy root checks remain")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
