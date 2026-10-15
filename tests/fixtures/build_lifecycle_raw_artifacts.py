#!/usr/bin/env python3
"""Build and inspect genuine freestanding WOS lifecycle artifacts in a fresh directory.

This helper performs no CREATE, VM launch, runtime fault injection or census.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import signal
import struct
import subprocess
import time
import zlib


BASELINE_PATH = "build/goal39-evidence/checkpoint41-tiny-artifacts-before01.json"
BASELINE_SHA = "b431eb44c3e2004bd9c342b3094cac493f5ffb265e8058823bf5245f8a99986e"
CORRECTION_PATH = "goals/progress/39/supplement-41-tiny-artifact-phdr-correction.md"
CORRECTION_SHA = "337a2a91f16e11ca34f46b22022ac458a8e685e5f448ffb187bab4b4b1689f8c"
SOURCES = (
    "tests/fixtures/lifecycle_tiny_interp.S",
    "tests/fixtures/lifecycle_raw_start.S",
    "tests/fixtures/lifecycle_raw_work.cpp",
    "tests/fixtures/build_lifecycle_raw_artifacts.py",
    "tests/fixtures/lifecycle_raw_artifacts.md",
)
MAX_ARTIFACT = 65536
MAX_DIRECTORY_BYTES = 8 * 1024 * 1024
MAX_SOURCE_BYTES = 16 * 1024 * 1024
MAX_INCLUDE_FILES = 32768
MAX_INCLUDE_BYTES = 256 * 1024 * 1024
MAX_COMMAND_FILE_BYTES = 1024 * 1024
INTERPRETER = "/lib/lifecycle-tiny-ld.so"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def require(predicate, message):
    if not predicate:
        raise RuntimeError(message)


def description(path, root, complete=False):
    data = path.read_bytes()
    row = {"path": str(path.relative_to(root)), "bytes": len(data), "sha256": digest(data)}
    if complete:
        require(len(data) <= MAX_SOURCE_BYTES, f"source capture too large: {path}")
        row["zlib_base64_complete_bytes"] = base64.b64encode(zlib.compress(data, 9)).decode("ascii")
    return row


def stable(root, rows):
    for row in rows:
        path = root / row["path"]
        require(path.is_file(), f"input disappeared: {path}")
        require(digest(path.read_bytes()) == row["sha256"], f"input changed: {path}")


def baseline(root):
    data = (root / BASELINE_PATH).read_bytes()
    require(digest(data) == BASELINE_SHA, "immutable pre-edit capture hash mismatch")
    require(digest((root / CORRECTION_PATH).read_bytes()) == CORRECTION_SHA, "approved ET_EXEC PHDR correction changed")
    captured = json.loads(data)
    rows = captured["complete_original_source_bytes"]
    for row in rows:
        decoded = zlib.decompress(base64.b64decode(row["zlib_base64_complete_bytes"], validate=True))
        require(len(decoded) == row["bytes"] and digest(decoded) == row["sha256"], "corrupt complete-byte baseline")
        require((root / row["path"]).read_bytes() == decoded, f"baseline source mismatch: {row['path']}")
    require(captured["compiler_invoked"] is False and captured["implementation_performed"] is False, "invalid pre-edit provenance")
    stable(root, [captured["ordinary_interpreter"]])
    tools = captured["compiler_linker_inspection_identities"]
    for row in tools:
        path = root / row["path"]
        require(str(path.resolve(strict=True)) == row["resolved_path"], f"tool target changed: {path}")
        require((os.readlink(path) if path.is_symlink() else None) == row["symlink"], f"tool symlink changed: {path}")
        require(digest(path.read_bytes()) == row["sha256"], f"tool bytes changed: {path}")
    return rows, tools, captured["ordinary_interpreter"]


def include_snapshot(root):
    roots = (
        root / "toolchain/sysroot/include/c++/v1",
        root / "toolchain/sysroot/include",
        root / "toolchain/host/lib/clang/22/include",
    )
    paths = set()
    for directory in roots:
        require(directory.is_dir(), f"target include directory missing: {directory}")
        for path in directory.rglob("*"):
            if path.is_file():
                paths.add(path)
                require(len(paths) <= MAX_INCLUDE_FILES, "target include census exceeds bound")
    rows = []
    total = 0
    for path in sorted(paths):
        row = description(path, root)
        row["symlink"] = os.readlink(path) if path.is_symlink() else None
        row["resolved_path"] = str(path.resolve(strict=True))
        total += row["bytes"]
        require(total <= MAX_INCLUDE_BYTES, "target include byte census exceeds bound")
        rows.append(row)
    return rows


def stable_includes(root, rows):
    require(include_snapshot(root) == rows, "target include tree changed during artifact build")


def bounded_outputs(directory):
    total = 0
    for path in directory.rglob("*"):
        require(not path.is_symlink(), f"output symlink appeared: {path}")
        if path.is_file():
            total += path.stat().st_size
    require(total <= MAX_DIRECTORY_BYTES, f"artifact directory exceeds {MAX_DIRECTORY_BYTES} bytes")
    return total


def save_json(path, value):
    data = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
    require(len(data) <= MAX_DIRECTORY_BYTES, "receipt exceeds output budget")
    with path.open("xb") as stream:
        stream.write(data)


def command(argv, root, directory, name, commands):
    log = directory / f"{name}.log"
    require(not log.exists() and not log.is_symlink(), f"command log collision: {log}")
    bounded_outputs(directory)
    environment = {"PATH": str(root / "toolchain/host/bin") + ":/usr/bin:/bin", "LANG": "C", "LC_ALL": "C", "TMPDIR": str(directory / ".tmp")}
    record = {"argv": argv, "cwd": str(root), "environment": environment, "timeout_seconds": 30,
              "child_file_size_limit_bytes": MAX_COMMAND_FILE_BYTES, "name": name}
    commands.append(record)
    started = time.monotonic()
    try:
        with log.open("xb") as stream:
            with subprocess.Popen(argv, cwd=root, env=environment, stdin=subprocess.DEVNULL, stdout=stream, stderr=subprocess.STDOUT,
                                  start_new_session=True, preexec_fn=limit_child_files) as process:
                record["owned_process_pid"] = process.pid
                try:
                    returncode = process.wait(timeout=30)
                except BaseException:
                    if process.returncode is None:
                        try:
                            os.killpg(process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        process.wait(timeout=5)
                    raise
        record["returncode"] = returncode
    except subprocess.TimeoutExpired:
        record["timeout"] = True
        record["elapsed_seconds"] = time.monotonic() - started
        raise
    record["elapsed_seconds"] = time.monotonic() - started
    record["log"] = description(log, root)
    bounded_outputs(directory)
    require(returncode == 0, f"command failed: {name}; see {log}")


def limit_child_files():
    # Host-side compiler containment only; this is unrelated to raw WOS limits.
    resource.setrlimit(resource.RLIMIT_FSIZE, (MAX_COMMAND_FILE_BYTES, MAX_COMMAND_FILE_BYTES))


def contained(start, size, regions):
    return any(start >= begin and size <= end - start for begin, end in regions if start <= end)


def elf_gate(path, role):
    data = path.read_bytes()
    require(64 <= len(data) <= MAX_ARTIFACT, f"ELF size outside genuine fallback bound: {path}")
    header = struct.unpack_from("<16sHHIQQQIHHHHHH", data)
    ident, kind, machine, version, entry, phoff, shoff, flags, ehsize, phentsize, phnum, shentsize, shnum, shstr = header
    require(ident[:7] == b"\x7fELF\x02\x01\x01" and kind == (3 if role == "interpreter" else 2) and machine == 62 and version == 1,
            f"wrong ELF identity/role: {path}")
    require(ehsize == 64 and phentsize == 56 and 1 <= phnum <= 32 and phoff >= 64 and phoff + phnum * 56 <= len(data), "invalid PHDR table")
    require(shentsize == 64 and 1 <= shnum <= 512 and 0 < shstr < shnum and shoff >= 64 and shoff + shnum * 64 <= len(data), "invalid section table")
    programs = [struct.unpack_from("<IIQQQQQQ", data, phoff + i * 56) for i in range(phnum)]
    loads = []
    readable_file_ranges = []
    executable_ranges = []
    interpreter = []
    stack = []
    dynamic = []
    for index, p in enumerate(programs):
        ptype, pflags, offset, vaddr, paddr, filesz, memsz, alignment = p
        require(offset <= len(data) and filesz <= len(data) - offset and filesz <= memsz, f"invalid program range {index}")
        require(vaddr < (1 << 47) and memsz <= (1 << 47) - vaddr, f"invalid program address {index}")
        if ptype == 1:
            require(pflags & 4 and not (pflags & 3 == 3) and memsz > 0, "unreadable/RWX/empty load")
            require(alignment in (0, 1) or (alignment <= 2 * 1024 * 1024 and alignment & (alignment - 1) == 0), "invalid load alignment")
            require((offset & 4095) == (vaddr & 4095) and (alignment <= 1 or offset % alignment == vaddr % alignment), "incongruent load")
            begin, end = vaddr & ~4095, (vaddr + memsz + 4095) & ~4095
            require(not any(begin < old_end and old_begin < end for old_begin, old_end in loads), "overlapping load pages")
            loads.append((begin, end))
            if not pflags & 1:
                readable_file_ranges.append((vaddr, vaddr + filesz))
            if pflags & 1:
                executable_ranges.append((vaddr, vaddr + filesz))
            if role != "interpreter":
                require(0x400000 <= begin and end <= 0x800000, "main outside fixed image window")
        elif ptype == 3:
            require(1 <= filesz <= 512 and data[offset + filesz - 1] == 0 and b"\0" not in data[offset:offset + filesz - 1], "malformed PT_INTERP")
            interpreter.append(data[offset:offset + filesz - 1].decode("ascii"))
        elif ptype == 2:
            dynamic.append((offset, filesz, vaddr))
        elif ptype == 7:
            raise RuntimeError("TLS is not admitted")
        elif ptype == 0x6474E551:
            require(not pflags & 1, "executable GNU_STACK")
            stack.append(p)
    require(loads and contained(entry, 1, executable_ranges), "entry is not file-backed executable code")
    require(len(stack) == 1, "missing/duplicate explicit NX GNU_STACK")
    if role == "interpreter":
        require(not interpreter and 0 < entry <= MAX_ARTIFACT, "interpreter path/entry invalid")
        require(any(p[0] == 1 and p[2] == 0 and p[3] == 0 and p[1] == 4 and p[5] >= phoff + phnum * 56 for p in programs), "no readable vaddr0 ELF/PHDR header load")
    else:
        require(interpreter == ([INTERPRETER] if role == "tiny-main" else []), "wrong main interpreter shape")
        require(phoff == 64 and any(p[0] == 1 and p[2] == 0 and p[3] == 0x400000 and p[1] == 4 and
                                   p[5] >= 64 + phnum * 56 for p in programs), "main does not contain the exact real header bytes")
    # Loader metadata must be contained in nonexecuting readable file bytes.
    for p in programs:
        if p[0] in (2, 3, 6, 0x6474E550, 0x6474E552) and p[5]:
            require(contained(p[3], p[5], readable_file_ranges), "metadata outside readable nonexecuting load")
    sections = [struct.unpack_from("<IIQQQQIIQQ", data, shoff + i * 64) for i in range(shnum)]
    symbols = []
    for s in sections:
        name, stype, sflags, saddr, offset, size, link, info, alignment, entsize = s
        require(stype == 8 or (offset <= len(data) and size <= len(data) - offset), "section bytes outside ELF")
        require(not (stype in (4, 9, 19, 14, 15, 16) and size), "relocations or constructors are not admitted")
        if stype in (2, 11):
            require(entsize == 24 and size % 24 == 0 and link < shnum, "invalid symbol table")
            strings = sections[link]
            require(strings[1] == 3 and strings[4] <= len(data) and strings[5] <= len(data) - strings[4], "invalid symbol string table")
            string_data = data[strings[4]:strings[4] + strings[5]]
            for pos in range(offset, offset + size, 24):
                sn, si, so, ss, sv, sz = struct.unpack_from("<IBBHQQ", data, pos)
                require(sn < len(string_data), "symbol name outside string table")
                nul = string_data.find(b"\0", sn)
                require(nul >= sn, "unterminated symbol name")
                text = string_data[sn:nul].decode("ascii")
                require(not (ss == 0 and si >> 4 != 0), f"undefined nonlocal symbol: {text}")
                require(si & 15 not in (6, 10), "TLS/IFUNC symbol")
                symbols.append({"name": text, "value": sv, "size": sz, "binding": si >> 4, "type": si & 15, "visibility": so & 3, "section": ss})
    starts = [s for s in symbols if s["name"] == "_start" and s["section"] != 0]
    require(starts and all(s["value"] == entry and s["visibility"] == 2 and s["type"] == 2 for s in starts), "hidden resolved _start mismatch")
    forbidden_tags = {1, 2, 7, 8, 9, 12, 13, 17, 18, 19, 23, 25, 26, 27, 28, 32, 33, 35, 36, 37}
    dynamic_rows = []
    require(len(dynamic) <= 1, "duplicate dynamic segment")
    for offset, size, vaddr in dynamic:
        require(size > 0 and size % 16 == 0, "invalid dynamic table")
        terminated = False
        for pos in range(offset, offset + size, 16):
            tag, value = struct.unpack_from("<qQ", data, pos)
            if tag == 0:
                terminated = True
                require(all(byte == 0 for byte in data[pos:offset + size]), "nonzero data after DT_NULL")
                break
            require(tag not in forbidden_tags, f"runtime dependency/relocation/initializer tag: {tag}")
            if tag in (4, 5, 6, 0x6FFFFEF5):
                require(contained(value, 1, readable_file_ranges), "dynamic metadata pointer outside readable load")
            dynamic_rows.append({"tag": tag, "value": value})
        require(terminated, "unterminated dynamic table")
    return {"path": str(path), "bytes": len(data), "sha256": digest(data), "role": role, "elf_type": kind, "entry": entry,
            "phoff": phoff, "phnum": phnum, "phentsize": phentsize, "expected_native_at_phdr": 0x1040 if role != "interpreter" else None,
            "loads": loads, "programs": programs, "interpreter": interpreter, "dynamic": dynamic_rows,
            "symbols": symbols, "structured_elf_gate": "PASS", "private_startup_qualified": False}


def instructions(path):
    result = []
    for line in path.read_text().splitlines():
        match = re.match(r"^\s*([0-9a-fA-F]+):\s+([a-zA-Z][a-zA-Z0-9.]*)\s*(.*)$", line)
        if match:
            result.append((int(match[1], 16), match[2], match[3].split("#", 1)[0].strip()))
    require(result, f"no parsed instructions: {path}")
    return result


def entry_disassembly_gate(path, elf, role):
    parsed = instructions(path)
    starts = [s for s in elf["symbols"] if s["name"] == "_start"]
    size = next(s["size"] for s in starts if s["size"] > 0)
    entry = elf["entry"]
    body = [row for row in parsed if entry <= row[0] < entry + size]
    require(body and body[0][0] == entry, "disassembly misses actual entry")
    if role == "interpreter":
        transfers = [i for i, row in enumerate(body) if row[1].startswith("jmp") and row[2] == "*%r8"]
        require(len(transfers) == 1, "missing/duplicate exact AT_ENTRY register jump")
        prefix = body[:transfers[0] + 1]
        for _, mnemonic, operands in prefix:
            require(not mnemonic.startswith(("call", "push", "pop")) and mnemonic not in ("syscall", "sysenter", "int"), "prehandoff call/stack/syscall")
            last = operands.rsplit(",", 1)[-1].strip()
            if not mnemonic.startswith(("cmp", "test", "j", "nop")):
                require(last not in ("%rsp", "%esp", "%sp", "%spl", "%rdi", "%edi", "%di", "%dil", "%rsi", "%esi", "%si", "%sil",
                                     "%rdx", "%edx", "%dx", "%dl", "%dh"), "opaque GPR/RSP write before handoff")
                require("(" not in last, "memory write before handoff")
        require(any("%r12" in row[2] and row[1].startswith("movabs") for row in prefix), "missing handoff marker")
    else:
        calls = [i for i, row in enumerate(body) if row[1].startswith("call")]
        require(calls and "lifecycle_raw_main" in body[calls[0]][2], "first entry call is not raw helper")
        prefix = body[:calls[0]]
        if role == "tiny-main":
            require(any(row[1].startswith("cmp") and row[2].endswith("%r12") for row in prefix), "missing incoming marker check")
            require(any(row[1].startswith("cmp") and "%r15" in row[2] for row in prefix), "missing incoming RSP check")
        for _, mnemonic, operands in prefix:
            require(mnemonic != "syscall", "syscall before completed startup checks")
            if not mnemonic.startswith(("cmp", "test", "j")):
                require(operands.rsplit(",", 1)[-1].strip() not in ("%r12", "%r12d", "%r12w", "%r12b"), "main initializes interpreter marker")
    return {"automatic_entry_disassembly_gate": "PASS", "requires_independent_source_and_disassembly_review": True,
            "runtime_startup_qualified": False}


def stack_gate(path):
    rows = []
    total = 208  # Raw assembly's fixed private Startup record.
    for line in path.read_text().splitlines():
        fields = line.split("\t")
        require(len(fields) == 3 and fields[1].isdigit() and fields[2] == "static", "unbounded or malformed compiler stack usage")
        amount = int(fields[1])
        total += amount
        rows.append({"function": fields[0], "frame_bytes": amount, "kind": fields[2]})
    require(rows and total <= 16384, "conservative sum of all raw stack frames exceeds16KiB")
    return {"rows": rows, "all_function_frames_plus_startup_bytes": total, "bound_bytes": 16384,
            "requires_source_review_of_nonrecursion": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    evidence = (root / "build/goal39-evidence").resolve(strict=True)
    output = args.output_dir.absolute()
    require(output.parent.resolve(strict=True) == evidence, "output must be one fresh direct child of build/goal39-evidence")
    require(output.name not in (".", "..") and not output.exists() and not output.is_symlink(), "output already exists or is unsafe")
    old_rows, tools, ordinary = baseline(root)
    sources = [description(root / name, root, True) for name in SOURCES]
    correction = description(root / CORRECTION_PATH, root, True)
    headers = include_snapshot(root)
    output.mkdir(mode=0o700, exist_ok=False)
    (output / ".tmp").mkdir(mode=0o700, exist_ok=False)
    commands = []
    receipt = {"kind": "WOS_RAW_LIFECYCLE_ARTIFACT_BUILD", "baseline_path": BASELINE_PATH, "baseline_sha256": BASELINE_SHA,
               "sources": sources, "approved_phdr_correction": correction,
               "baseline_inputs": [{k: v for k, v in row.items() if k != "zlib_base64_complete_bytes"} for row in old_rows],
               "tools": tools, "target_include_inputs": headers, "ordinary_interpreter": ordinary, "commands": commands,
               "built": False, "elf_and_disassembly_validated": False, "runtime_performed": False, "private_startup_qualified": False,
               "stage_consumption_qualified": False, "controller_census_qualified": False, "output_directory": str(output)}
    save_json(output / "source-before.json", receipt)
    clang = str(root / "toolchain/host/bin/clang")
    clangxx = str(root / "toolchain/host/bin/clang++")
    linker = str(root / "toolchain/host/bin/ld.lld")
    readobj = str(root / "toolchain/host/bin/llvm-readobj")
    objdump = str(root / "toolchain/host/bin/llvm-objdump")
    sysroot = root / "toolchain/sysroot"
    assembly = [clang, "--no-default-config", "--target=x86_64-pc-wos", f"--sysroot={sysroot}", "-c", "-x", "assembler-with-cpp"]
    common_link = [linker, "-m", "elf_x86_64", "--no-undefined", "-e", "_start", "-z", "separate-code", "-z", "max-page-size=4096",
                   "-z", "noexecstack", "--build-id=none"]
    try:
        for label, tool in (("clang", clang), ("clangxx", clangxx), ("lld", linker), ("readobj", readobj), ("objdump", objdump)):
            command([tool, "--version"], root, output, f"version-{label}", commands)
        command(assembly + [str(root / SOURCES[0]), "-o", str(output / "interp.o")], root, output, "assemble-interpreter", commands)
        for mode in (1, 2):
            command(assembly + [f"-DSTARTUP_MODE={mode}", str(root / SOURCES[1]), "-o", str(output / f"start-{mode}.o")], root, output,
                    f"assemble-start-{mode}", commands)
        compile_work = [clangxx, "--no-default-config", "--target=x86_64-pc-wos", f"--sysroot={sysroot}", "-std=c++23", "-O2", "-ffreestanding", "-fno-builtin",
                        "-fno-pic", "-fno-pie", "-fno-exceptions", "-fno-rtti", "-fno-stack-protector", "-fno-stack-check",
                        "-fno-unwind-tables", "-fno-asynchronous-unwind-tables", "-fno-sanitize=safe-stack", "-mno-red-zone", "-mno-mmx",
                        "-mno-sse", "-mno-sse2", "-fno-vectorize", "-fno-slp-vectorize", "-fstack-usage", "-Wall", "-Wextra", "-Werror", "-nostdinc", "-nostdinc++",
                        "-isystem", str(sysroot / "include/c++/v1"), "-isystem", str(root / "toolchain/host/lib/clang/22/include"),
                        "-isystem", str(sysroot / "include"), "-I", str(root / "modules/kern/src"), "-c", str(root / SOURCES[2]),
                        "-o", str(output / "work.o")]
        command(compile_work, root, output, "compile-work", commands)
        receipt["conservative_stack_gate"] = stack_gate(output / "work.su")
        command(common_link + ["-shared", "--no-dynamic-linker", str(output / "interp.o"), "-o", str(output / "lifecycle-tiny-ld.so")],
                root, output, "link-interpreter", commands)
        for mode, name, option in ((1, "lifecycle-raw-static", "--no-dynamic-linker"),
                                   (2, "lifecycle-raw-tiny", f"--dynamic-linker={INTERPRETER}")):
            command(common_link + ["--image-base=0x400000", option, str(output / f"start-{mode}.o"), str(output / "work.o"), "-o", str(output / name)],
                    root, output, f"link-main-{mode}", commands)
        artifacts = []
        for name, role in (("lifecycle-tiny-ld.so", "interpreter"), ("lifecycle-raw-static", "static-main"), ("lifecycle-raw-tiny", "tiny-main")):
            path = output / name
            metadata = elf_gate(path, role)
            command([readobj, "--file-headers", "--program-headers", "--dynamic-table", "--relocations", "--symbols", str(path)], root, output,
                    f"inspect-{name}", commands)
            command([objdump, "-d", "--no-show-raw-insn", str(path)], root, output, f"disassemble-{name}", commands)
            metadata["entry_disassembly"] = entry_disassembly_gate(output / f"disassemble-{name}.log", metadata, role)
            artifacts.append(metadata)
        receipt["artifacts"] = artifacts
        receipt["built"] = True
        receipt["elf_and_disassembly_validated"] = True
        stable(root, old_rows + sources + [ordinary, correction])
        stable_includes(root, headers)
        for tool in tools:
            require(digest((root / tool["path"]).read_bytes()) == tool["sha256"] and str((root / tool["path"]).resolve(strict=True)) == tool["resolved_path"],
                    "tool changed during build")
        receipt["inputs_stable"] = True
        receipt["result"] = "PASS_ARTIFACT_ONLY"
        bounded_outputs(output)
    except Exception as error:
        receipt["result"] = "FAIL"
        receipt["error"] = f"{type(error).__name__}: {error}"
        receipt["failure_outputs_retained"] = True
    save_json(output / "artifact-receipt.json", receipt)
    bounded_outputs(output)
    print(json.dumps({"result": receipt["result"], "receipt": str(output / "artifact-receipt.json"),
                      "runtime_performed": False, "private_startup_qualified": False}, sort_keys=True))
    return 0 if receipt["result"] == "PASS_ARTIFACT_ONLY" else 1


if __name__ == "__main__":
    raise SystemExit(main())
