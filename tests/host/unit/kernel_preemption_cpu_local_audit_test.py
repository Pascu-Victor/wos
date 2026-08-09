#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
KERNEL_SOURCE = ROOT / "modules" / "kern" / "src"
AUDIT = ROOT / "docs" / "kernel_preemption_cpu_local_audit.md"

CPU_LOCAL_PATTERNS = (
    "cpu::current_cpu(",
    "cpu::get_current_cpu_id_safe(",
    "get_current_task(",
    "->this_cpu(",
    ".this_cpu(",
    "IA32_KERNEL_GS_BASE",
    "rdgsbase",
    "wrgsbase",
    "swapgs",
    "gs:0x",
    "%gs",
)
CLASSIFICATIONS = ("re-read", "migration-protected", "preemption-protected", "refactored")
SOURCE_SUFFIXES = {".c", ".cpp", ".h", ".hpp", ".asm"}


def audited_source_files() -> set[str]:
    files: set[str] = set()
    for path in KERNEL_SOURCE.rglob("*"):
        if not path.is_file() or path.suffix not in SOURCE_SUFFIXES:
            continue
        source = path.read_text(errors="replace")
        if any(pattern in source for pattern in CPU_LOCAL_PATTERNS):
            files.add(path.relative_to(ROOT).as_posix())
    return files


def ledger_entries(source: str) -> dict[str, str]:
    entries: dict[str, str] = {}
    for line in source.splitlines():
        match = re.match(r"- `([^`]+)` — (.+)$", line)
        if match is not None:
            entries[match.group(1)] = match.group(2)
    return entries


def main() -> None:
    audit_source = AUDIT.read_text()
    entries = ledger_entries(audit_source)
    discovered = audited_source_files()

    missing = sorted(discovered - entries.keys())
    if missing:
        raise AssertionError("unclassified CPU-local source files: " + ", ".join(missing))

    invalid = sorted(path for path in discovered if not any(kind in entries[path] for kind in CLASSIFICATIONS))
    if invalid:
        raise AssertionError("CPU-local audit entries without a valid classification: " + ", ".join(invalid))

    stale = sorted(path for path in entries if path.startswith("modules/kern/src/") and not (ROOT / path).is_file())
    if stale:
        raise AssertionError("stale CPU-local audit paths: " + ", ".join(stale))

    print(f"CPU-local preemption audit covers {len(discovered)} source files")


if __name__ == "__main__":
    main()
