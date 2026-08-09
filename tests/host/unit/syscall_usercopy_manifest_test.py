#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MANIFEST = ROOT / "docs" / "syscall_usercopy_manifest.md"

ENUM_SPECS = {
    "SYS_LOG": ("modules/kern/src/abi/callnums/sys_log.h", "sys_log_ops"),
    "FUTEX": ("modules/kern/src/abi/callnums/futex.h", "futex_ops"),
    "THREAD_INFO": ("modules/kern/src/abi/callnums/multiproc.h", "threadInfoOps"),
    "THREAD_CONTROL": ("modules/kern/src/abi/callnums/multiproc.h", "threadControlOps"),
    "PROCESS": ("modules/kern/src/abi/callnums/process.h", "procmgmt_ops"),
    "TIME": ("modules/kern/src/abi/callnums/time.h", "sys_time_ops"),
    "VFS": ("modules/kern/src/abi/callnums/vfs.h", "ops"),
    "NET": ("modules/kern/src/abi/callnums/net.h", "ops"),
    "VMEM": ("modules/kern/src/abi/callnums/vmem.h", "ops"),
    "SHM": ("modules/kern/src/abi/callnums/shm.h", "ops"),
    "POWER": ("modules/kern/src/abi/callnums/power.h", "ops"),
    "PTRACE": ("modules/kern/src/abi/ptrace.hpp", "request"),
}

EXPANDED_TOP_LEVEL = {
    "SYS_LOG",
    "FUTEX",
    "THREADING",
    "PROCESS",
    "TIME",
    "VFS",
    "NET",
    "VMEM",
    "SHM",
    "POWER",
}
DIRECT_TOP_LEVEL = {"VMEM_MAP", "DEBUG", "PERSONALITY"}
VALID_ACCESS = {"none", "in", "out", "in/out", "address", "range"}

ROW_RE = re.compile(
    r"^\|\s*`(?P<family>[A-Z_]+)`\s*\|\s*`(?P<operation>[A-Z0-9_]+)`\s*"
    r"\|\s*`(?P<access>[^`]+)`\s*\|(?P<bound>[^|]+)\|(?P<nullable>[^|]+)\|"
    r"(?P<partial>[^|]+)\|(?P<effects>[^|]+)\|\s*$",
    flags=re.MULTILINE,
)


def fail(message: str) -> None:
    raise AssertionError(message)


def enum_members(path: Path, enum_name: str) -> set[str]:
    source = path.read_text()
    match = re.search(rf"enum\s+class\s+{re.escape(enum_name)}\s*:[^{{]+\{{(.*?)\}};", source, flags=re.DOTALL)
    if match is None:
        fail(f"missing enum class {enum_name} in {path.relative_to(ROOT)}")
    body = re.sub(r"//[^\n]*", "", match.group(1))
    body = re.sub(r"/\*.*?\*/", "", body, flags=re.DOTALL)
    members: set[str] = set()
    for entry in body.split(","):
        name = entry.split("=", maxsplit=1)[0].strip()
        if not name:
            continue
        if re.fullmatch(r"[A-Z][A-Z0-9_]*", name) is None:
            fail(f"unparsed {enum_name} member {name!r} in {path.relative_to(ROOT)}")
        members.add(name)
    return members


def test_manifest_covers_current_abi_enums() -> None:
    text = MANIFEST.read_text()
    rows: dict[str, dict[str, dict[str, str]]] = {}
    for match in ROW_RE.finditer(text):
        row = {key: value.strip() for key, value in match.groupdict().items()}
        family = row["family"]
        operation = row["operation"]
        if family not in ENUM_SPECS and family != "TOP":
            continue
        if operation in rows.setdefault(family, {}):
            fail(f"duplicate manifest row {family}.{operation}")
        if row["access"] not in VALID_ACCESS:
            fail(f"invalid access class {row['access']!r} for {family}.{operation}")
        for field in ("bound", "nullable", "partial", "effects"):
            if not row[field] or "TBD" in row[field].upper():
                fail(f"missing {field} contract for {family}.{operation}")
        if row["access"] == "none" and row["bound"].lower() != "n/a":
            fail(f"pointer-free {family}.{operation} must use an n/a bound")
        rows[family][operation] = row

    for family, (relative_path, enum_name) in ENUM_SPECS.items():
        expected = enum_members(ROOT / relative_path, enum_name)
        actual = set(rows.get(family, {}))
        if actual != expected:
            fail(
                f"{family} manifest drift: missing={sorted(expected - actual)}, "
                f"extra={sorted(actual - expected)}"
            )

    actual_direct = set(rows.get("TOP", {}))
    if actual_direct != DIRECT_TOP_LEVEL:
        fail(
            f"direct top-level manifest drift: missing={sorted(DIRECT_TOP_LEVEL - actual_direct)}, "
            f"extra={sorted(actual_direct - DIRECT_TOP_LEVEL)}"
        )


def test_every_top_level_syscall_is_expanded_or_direct() -> None:
    callnums = enum_members(ROOT / "modules/kern/src/abi/callnums.hpp", "callnums")
    accounted = EXPANDED_TOP_LEVEL | DIRECT_TOP_LEVEL
    if callnums != accounted:
        fail(
            f"top-level syscall manifest drift: missing={sorted(callnums - accounted)}, "
            f"stale={sorted(accounted - callnums)}"
        )


def test_command_dependent_pointer_contracts_are_explicit() -> None:
    text = MANIFEST.read_text()
    required = [
        "F_GETLK/F_OFD_GETLK",
        "F_SETLK/F_SETLKW/F_OFD_SETLK/F_OFD_SETLKW",
        "TIOCGPTN",
        "TCGETS",
        "PR_SET_NAME",
        "ARCH_GET_FS/ARCH_GET_GS",
        "SIGALTSTACK nested address",
        "VMEM_MAP and VMEM address arguments",
        "No usercopy while locked",
    ]
    missing = [token for token in required if token not in text]
    if missing:
        fail(f"manifest is missing command-dependent contracts: {', '.join(missing)}")


def main() -> None:
    test_manifest_covers_current_abi_enums()
    test_every_top_level_syscall_is_expanded_or_direct()
    test_command_dependent_pointer_contracts_are_explicit()
    print("syscall usercopy manifest covers the current ABI")


if __name__ == "__main__":
    main()
