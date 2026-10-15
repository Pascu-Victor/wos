#!/usr/bin/env python3
"""Build the hosted WOS raw-parent pilot from immutable, reviewed raw artifacts.

This helper creates only a fresh, bounded artifact directory. It does not build
the kernel, change the sysroot, run the target executable or launch a VM.
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


ADMISSION = ("build/goal39-evidence/checkpoint41-raw-parent-pilot-root-admission01.json",
             "6aaf7c598a08c3f7841c76870889bf349ed51ef8aac30ee4796f307a9bd01542")
ARTIFACT_RECEIPT_SHA = "545f9ab610a7c5091627d0203a0732e00137f669663493ea370689f48c51634c"
ARTIFACT_REVIEW_SHA = "c30ff697c75143ae27d7987e9137a2c4750baf284491e3ef9cbbd7cbaf349b14"
SOURCE_REVIEW = ("build/goal39-evidence/checkpoint41-tiny-artifacts-independent-review01.json",
                 "9505b4ca11c8a2e13bf3e9344e762662d2d45146564254fe91db5fc895710563")
SUCCESSOR_ADMISSION = ("build/goal39-evidence/checkpoint41-raw-parent-pilot-successor-root-admission01.json",
                       "0780f9fef1e9175bfc1783a632db9ac80f165241b835bf532d940c6bb7cf74a3")
CONTEXT_PATHS = {"modules/kern/src/platform/sched/threading.cpp", "modules/kern/src/platform/sched/threading.hpp",
                 "modules/kern/src/platform/sys/context_switch.cpp"}
TINY_CONTEXT = "modules/kern/src/platform/sys/context_switch.cpp"
SOURCES = ("tests/fixtures/lifecycle_raw_pilot.cpp", "tests/fixtures/build_lifecycle_raw_pilot.py",
           "tests/fixtures/lifecycle_raw_pilot.md")
RAW_SOURCES = ("tests/fixtures/lifecycle_tiny_interp.S", "tests/fixtures/lifecycle_raw_start.S",
               "tests/fixtures/lifecycle_raw_work.cpp", "tests/fixtures/build_lifecycle_raw_artifacts.py",
               "tests/fixtures/lifecycle_raw_artifacts.md")
# These are the actual, independently reviewed artifact identities, not a model
# of a future compiler result. A different artifact requires a new admission.
ARTIFACTS = {
    "static-main": ("STATIC", "lifecycle-raw-static", 17568, 0x401000, 4,
                    "369c754e0983b108cf18bfec71afdfbe27032b1da2705cd05dc42d0803e58b4b"),
    "tiny-main": ("TINY", "lifecycle-raw-tiny", 17640, 0x401000, 5,
                  "6de43dc3ceeade091fc275dc6b9e0c4cf419afc68594a07f332d4a2b1c38d740"),
    "interpreter": ("INTERP", "lifecycle-tiny-ld.so", 9376, 0x1000, 7,
                    "4920a86f50397ba1b60e7e502bf9b25512c5eb8825630844d69b218963efbc97"),
}
MAX_DIRECTORY_BYTES = 8 * 1024 * 1024
MAX_COMMAND_FILE_BYTES = 1024 * 1024
MAX_INPUT_FILE_BYTES = 256 * 1024 * 1024
MAX_HEADER_FILES = 32768
MAX_HEADER_BYTES = 256 * 1024 * 1024
MAX_OUTPUT_FILES = 128


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def input_path(root, name):
    path = Path(name)
    if not path.is_absolute():
        path = root / path
    require(path == Path(os.path.abspath(path)) and path.is_relative_to(root), f"input outside repository: {path}")
    require(path.is_file() and path.resolve(strict=True).is_relative_to(root), f"input is not a repository file: {path}")
    return path


def describe(path, root):
    path = input_path(root, path)
    size = path.stat().st_size
    require(size <= MAX_INPUT_FILE_BYTES, f"input file exceeds bound: {path}")
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            checksum.update(chunk)
    require(path.stat().st_size == size, f"input size changed while reading: {path}")
    return {"path": str(path.relative_to(root)), "bytes": size, "sha256": checksum.hexdigest(),
            "resolved_path": str(path.resolve(strict=True)), "symlink": os.readlink(path) if path.is_symlink() else None}


def hold(root, row, pins):
    current = describe(input_path(root, row["path"]), root)
    for key in ("bytes", "sha256", "resolved_path", "symlink"):
        if key in row:
            require(current[key] == row[key], f"input {key} changed: {row['path']}")
    old = pins.setdefault(current["path"], current)
    require(old == current, f"conflicting input identity: {current['path']}")
    return current


def read_json(root, path, expected_sha, pins):
    row = hold(root, {"path": str(path), "sha256": expected_sha}, pins)
    require(row["bytes"] <= MAX_DIRECTORY_BYTES, f"JSON input exceeds bound: {path}")
    data = (root / row["path"]).read_bytes()
    require(digest(data) == expected_sha, f"JSON changed while reading: {path}")
    return json.loads(data), row


def stable(root, pins):
    for row in pins.values():
        require(describe(root / row["path"], root) == row, f"input changed during build: {row['path']}")


def header_paths(root, kernel=False):
    directories = (("modules/kern/src",) if kernel else
                   ("toolchain/sysroot/include", "toolchain/host/lib/clang/22/include"))
    paths = set()
    for relative in directories:
        directory = root / relative
        require(directory.is_dir() and not directory.is_symlink(), f"header tree missing or redirected: {directory}")
        for path in directory.rglob("*"):
            if path.is_file() and (not kernel or path.suffix in (".h", ".hpp")):
                paths.add(str(path.relative_to(root)))
                require(len(paths) <= MAX_HEADER_FILES, "header census exceeds file bound")
    return sorted(paths)


def header_snapshot(root, paths, pins):
    rows = [hold(root, describe(root / path, root), pins) for path in paths]
    require(sum(row["bytes"] for row in rows) <= MAX_HEADER_BYTES, "header census exceeds byte bound")
    return rows


def validate_source_successor(root, path, expected_sha, pins):
    require(re.fullmatch(r"[0-9a-f]{64}", expected_sha) is not None, "invalid explicit successor SHA256")
    path = input_path(root, path)
    require(path.is_relative_to(root / "build/goal39-evidence") and not path.is_symlink(), "successor must be an exclusive repository evidence file")
    manifest, manifest_pin = read_json(root, path, expected_sha, pins)
    require(manifest["schema_version"] == 1 and manifest["qualification"] == "ROOT_BOUND_PILOT_SOURCE_SUCCESSOR_ONLY" and
            manifest["errors"] == [], "invalid explicit source successor")
    manifest_rows = [manifest[key] for key in ("root_admission", "current_source", "current_preservation", "parent_source_review", "after_layout")]
    manifest_rows += manifest["parent_sources"] + list(manifest["commands"].values()) + list(manifest["kernels"].values())
    require(all({"path", "bytes", "sha256"}.issubset(row) and isinstance(row["bytes"], int) and not isinstance(row["bytes"], bool) and
                row["bytes"] >= 0 and re.fullmatch(r"[0-9a-f]{64}", row["sha256"]) is not None for row in manifest_rows),
            "successor pin lacks exact hash/size identity")
    admission, admission_pin = read_json(root, root / SUCCESSOR_ADMISSION[0], SUCCESSOR_ADMISSION[1], pins)
    require(hold(root, manifest["root_admission"], pins) == admission_pin, "successor does not bind exact root admission")
    require(admission["qualification"] == "ADMIT_NARROW_PARENT_BUILDER_CONTEXT_SUCCESSOR_SOURCE_ONLY" and
            admission["allowed_files"] == list(SOURCES[1:]) and
            set(admission["allowed_historical_context_paths"]) == CONTEXT_PATHS, "wrong successor admission scope")
    for key in ("contract", "facts"):
        hold(root, admission[key], pins)
    facts = json.loads(input_path(root, admission["facts"]["path"]).read_bytes())
    fact_commands = {Path(row["path"]).name: row for row in facts["fresh_current_command_receipts"]}
    require(len(fact_commands) == len(facts["fresh_current_command_receipts"]) == 5, "wrong admitted command/layout facts")
    compile_correction, _ = read_json(root, root / "build/goal39-evidence/checkpoint41-raw-parent-pilot-compile-correction01.json",
                                      "607bb11d1e8715c0205fb7a801d03793ff1b1d1137ba2e4e29afb0522d717371", pins)
    require(compile_correction["qualification"] == "ROOT_LITERAL_LOCAL_NAME_PILOT_COMPILE_CORRECTION_ONLY" and
            compile_correction["errors"] == [] and compile_correction["original_cpp"] == admission["current_three_sources"][0],
            "unadmitted parent compile correction")
    hold(root, compile_correction["contract"], pins)
    hold(root, compile_correction["corrected_cpp"], pins)
    current, current_pin = read_json(root, root / admission["current_source"]["path"], admission["current_source"]["sha256"], pins)
    proof, proof_pin = read_json(root, root / admission["current_preservation"]["path"], admission["current_preservation"]["sha256"], pins)
    require(hold(root, manifest["current_source"], pins) == current_pin and
            hold(root, manifest["current_preservation"], pins) == proof_pin, "successor does not bind admitted current proof")
    require(proof["qualification"] == "PASS" and proof["kind"] == "COMPOSITE_CURRENT_SOURCE_PRESERVATION_ONLY" and
            proof["errors"] == [] and proof["complete_current_source_count"] == 618 and
            len(proof["checks"]) == 7795 and all(row["pass"] is True for row in proof["checks"]), "incomplete inverse/format/current proof")
    require(current["schema_version"] == 1 and current["qualification"] == "CURRENT_SOURCE_CAPTURE_ONLY" and
            current["complete_source_count"] == len(current["complete_sources"]) == 618, "wrong current source census")
    for row in proof["inputs"] + proof["source_pins"]:
        hold(root, row, pins)
    require(len(proof["source_pins"]) == len({row["path"] for row in proof["source_pins"]}) == 628, "duplicate current proof pins")

    def decoded(row):
        raw = zlib.decompress(base64.b64decode(row["zlib_base64"], validate=True))
        require(len(raw) == row["bytes"] and digest(raw) == row["sha256"], "invalid complete source bytes")
        return raw

    old, _ = read_json(root, root / "build/goal39-evidence/checkpoint39-usercopy-pin-current-source01.json",
                      "95923b08bd950f09a6a8bd50cd903c14841a9bf2d570298b5c8ce7a944fe86fb", pins)
    old_rows, current_rows = old["complete_sources"], current["complete_sources"]
    require(len(old_rows) == old["complete_source_count"] == 617 and
            set(current_rows) == set(old_rows) | {"modules/kern/src/test/user_stack_return_ktest.cpp"}, "unexpected source successor paths")
    changed = set()
    for relative, row in current_rows.items():
        require(not Path(relative).is_absolute() and ".." not in Path(relative).parts and
                Path(row["path"]) == root / relative and not (root / relative).is_symlink(), "redirected current source")
        raw = decoded(row)
        require((root / relative).read_bytes() == raw, f"current complete source changed: {relative}")
        hold(root, row, pins)
        if relative in old_rows and raw != decoded(old_rows[relative]):
            changed.add(relative)
    require(changed == CONTEXT_PATHS, "additional or missing contextual source drift")
    tiny_before, _ = read_json(root, root / "build/goal39-evidence/checkpoint41-tiny-artifacts-before01.json",
                              "b431eb44c3e2004bd9c342b3094cac493f5ffb265e8058823bf5245f8a99986e", pins)
    tiny_rows = [row for row in tiny_before["complete_original_source_bytes"] if row["path"] == TINY_CONTEXT]
    require(len(tiny_rows) == 1, "missing complete Tiny context stream")
    tiny_bytes = zlib.decompress(base64.b64decode(tiny_rows[0]["zlib_base64_complete_bytes"], validate=True))
    require(len(tiny_bytes) == tiny_rows[0]["bytes"] and digest(tiny_bytes) == tiny_rows[0]["sha256"] and
            tiny_bytes == decoded(old_rows[TINY_CONTEXT]), "Tiny context differs from admitted original stream")

    review_pin = hold(root, manifest["parent_source_review"], pins)
    source_review = json.loads((root / review_pin["path"]).read_bytes())
    require(source_review["qualification"] == "CLEAR_PARENT_BUILDER_SOURCE_SUCCESSOR_ONLY" and source_review["errors"] == [],
            "parent successor source review is not clear")
    require(all({"path", "bytes", "sha256"}.issubset(row) for row in source_review["source_pins"]), "source review lacks exact parent identities")
    parent_rows = manifest["parent_sources"]
    require(len(parent_rows) == 3 and {hold(root, row, pins)["path"] for row in parent_rows} == set(SOURCES), "wrong current parent sources")
    reviewed_rows = [hold(root, row, pins) for row in source_review["source_pins"]]
    require(len(reviewed_rows) == len({row["path"] for row in reviewed_rows}) and
            set(SOURCES).issubset({row["path"] for row in reviewed_rows}), "source review does not bind all parent sources")

    commands = manifest["commands"]
    require(set(commands) == {"normal_build", "diagnostic_build", "host", "format"}, "missing current qualification commands")
    command_names = {"normal_build": "user-stack-return-normal01-result.json", "diagnostic_build": "user-stack-return-diagnostic01-result.json",
                     "host": "user-stack-return-host01-result.json", "format": "user-stack-return-format02-result.json"}
    for role, row in commands.items():
        held = hold(root, row, pins)
        require(held == hold(root, fact_commands[command_names[role]], pins), "command differs from admitted exact facts")
        command_receipt = json.loads((root / held["path"]).read_bytes())
        require(command_receipt["exit"] == 0 and command_receipt["source_stable_during_command"] is True and
                command_receipt["source_before"] == command_receipt["source_after"], f"current command did not pass: {role}")
        source_rows = command_receipt["source_after"]
        require(len(source_rows) == 618 and {input_path(root, item["path"]) for item in source_rows} ==
                {root / relative for relative in current_rows}, "command does not cover exact current618")
        for item in source_rows + command_receipt["pins"]:
            hold(root, item, pins)
        argv = command_receipt["command"]
        if role in ("normal_build", "diagnostic_build"):
            require(held == hold(root, admission[role], pins), "build differs from admitted current build")
        elif role == "host":
            require(command_receipt["profile"] == "host" and argv[0] == "ctest" and "--no-tests=error" in argv and
                    "--output-on-failure" in argv, "wrong Host command")
            expression = argv[argv.index("-R") + 1]
            require(expression.startswith("^(") and expression.endswith(")$") and
                    len(set(expression[2:-2].split("|"))) == 60, "wrong Host60 census")
            logs = [item for item in command_receipt["pins"] if item["path"].endswith(".log")]
            require(len(logs) == 1, "wrong Host log census")
            text = input_path(root, logs[0]["path"]).read_text()
            summaries = re.findall(r"100% tests passed(?:, 0 tests failed)? out of (\d+)", text)
            require(summaries == ["60"] and all(re.search(r"\b" + re.escape(name) + r"\b[^\n]*\bPassed\b", text)
                    for name in expression[2:-2].split("|")), "Host60 actual log did not pass")
        else:
            offset = argv.index("scripts/dev/format_repo.sh")
            require(argv[offset + 1] == "--check" and CONTEXT_PATHS.issubset(set(argv[offset + 2:])), "wrong current formatter gate")
    layout_pin = hold(root, manifest["after_layout"], pins)
    require(layout_pin == hold(root, fact_commands["after-user-stack-return-layout02.json"], pins), "layout differs from admitted exact facts")
    layout = json.loads((root / layout_pin["path"]).read_bytes())
    require(layout["status"] == "PASS" and layout["errors"] == [] and set(layout["profiles"]) == {"normal", "diagnostic"} and
            layout["sources_before"] == layout["sources_after"], "current ABI proof did not pass")
    for profile in layout["profiles"].values():
        require(profile["status"] == "PASS" and profile["returncode"] == 0 and len(profile["layout"]) == 22 and
                profile["baseline_layout_and_compiler_shape_equal"] is True, "current layout/compiler changed")
        hold(root, profile["compiler"], pins)
    for row in layout["sources_after"] + [layout[key] for key in ("probe", "helper", "source_map", "baseline")]:
        hold(root, row, pins)
    require(set(manifest["kernels"]) == {"normal", "diagnostic"}, "missing current kernels")
    for profile, relative in (("normal", "build/modules/kern/wos"), ("diagnostic", "build-ktest/modules/kern/wos")):
        require(input_path(root, manifest["kernels"][profile]["path"]) == root / relative, "wrong current kernel path")
        hold(root, manifest["kernels"][profile], pins)
    return {"manifest": manifest_pin, "old_rows": old_rows, "current_rows": current_rows, "historical_context_inputs": []}


def hold_archived_context(root, row, pins, successor, collection):
    path = input_path(root, row["path"])
    relative = str(path.relative_to(root))
    allowed = CONTEXT_PATHS if collection == "old617" else {TINY_CONTEXT}
    require(collection in ("old617", "tiny_baseline51", "tiny_held3095"), "unexpected archive-row collection")
    if relative not in allowed:
        return hold(root, row, pins)
    require(not path.is_symlink() and path.resolve(strict=True) == path, "redirected contextual source")
    old = successor["old_rows"][relative]
    require(row["bytes"] == old["bytes"] and row["sha256"] == old["sha256"], "unapproved historical context version")
    key = (collection, relative)
    require(not any((item["collection"], item["path"]) == key for item in successor["historical_context_inputs"]),
            "duplicate historical context row")
    current = hold(root, successor["current_rows"][relative], pins)
    successor["historical_context_inputs"].append({"collection": collection, "path": relative,
                                                   "bytes": old["bytes"], "sha256": old["sha256"], "current": current})
    return current


def preflight(root, receipt_path, review_path, successor_path, successor_sha):
    pins = {}
    successor = validate_source_successor(root, successor_path, successor_sha, pins)
    admission, _ = read_json(root, root / ADMISSION[0], ADMISSION[1], pins)
    require(admission["qualification"] == "ADMIT_EXACT_THREE_NEW_SOURCE_FILES_ONLY" and not admission["errors"], "invalid source admission")
    require(admission["allowed_new_files"] == list(SOURCES) and admission["protected_complete_source_count"] == 617, "wrong admitted patch boundary")
    for key in ("contract", "reconnaissance", "proposal", "actual_tiny_ELF_review"):
        hold(root, admission[key], pins)
    proposal = json.loads((root / hold(root, admission["proposal"], pins)["path"]).read_bytes())
    for row in proposal["source_pins"] + proposal["predecessors"]:
        hold(root, row, pins)
    source_capsule = json.loads((root / proposal["predecessors"][0]["path"]).read_bytes())
    require(source_capsule["complete_source_count"] == 617 and len(source_capsule["complete_sources"]) == 617, "protected source census changed")
    for row in source_capsule["complete_sources"].values():
        hold_archived_context(root, row, pins, successor, "old617")
    source_review, _ = read_json(root, root / SOURCE_REVIEW[0], SOURCE_REVIEW[1], pins)
    require(source_review["qualification"] == "CLEAR_SOURCE_REVIEW_ONLY" and not source_review["errors"], "raw source review is not clear")
    for row in source_review["source_pins"] + source_review["input_pins"]:
        hold(root, row, pins)
    receipt, receipt_pin = read_json(root, receipt_path, ARTIFACT_RECEIPT_SHA, pins)
    review, review_pin = read_json(root, review_path, ARTIFACT_REVIEW_SHA, pins)
    require(review_pin["sha256"] == admission["actual_tiny_ELF_review"]["sha256"], "review differs from admitted review")
    require(receipt["kind"] == "WOS_RAW_LIFECYCLE_ARTIFACT_BUILD" and receipt["result"] == "PASS_ARTIFACT_ONLY", "actual raw artifact build did not pass")
    require(all(receipt[key] is True for key in ("built", "inputs_stable", "elf_and_disassembly_validated")), "incomplete artifact build gates")
    require(all(receipt[key] is False for key in ("runtime_performed", "private_startup_qualified", "stage_consumption_qualified", "controller_census_qualified")), "artifact receipt overclaims runtime")
    require(review["qualification"] == "INDEPENDENT_ACTUAL_ELF_AND_DISASSEMBLY_CLEAR_ARTIFACT_ONLY" and not review["errors"], "generated ELF/disassembly review is not clear")
    require(review["artifact_receipt"]["sha256"] == receipt_pin["sha256"] and input_path(root, review["artifact_receipt"]["path"]) == input_path(root, receipt_path), "review does not bind supplied receipt")
    require(review["all_actual_command_returncodes_zero"] is True and review["baseline51_and_source5_and_header_tool_inputs_current"] is True, "actual artifact review gates incomplete")
    require(len(receipt["commands"]) == review["actual_build_commands"] == 18, "wrong actual build command census")
    require(len(receipt["sources"]) == 5 and {row["path"] for row in receipt["sources"]} == set(RAW_SOURCES), "raw source census changed")
    for row in receipt["sources"] + receipt["tools"] + receipt["target_include_inputs"]:
        hold(root, row, pins)
    require(len(receipt["baseline_inputs"]) == len({str(input_path(root, row["path"])) for row in receipt["baseline_inputs"]}) == 51,
            "Tiny baseline51 census changed")
    for row in receipt["baseline_inputs"]:
        hold_archived_context(root, row, pins, successor, "tiny_baseline51")
    hold(root, receipt["ordinary_interpreter"], pins)
    hold(root, receipt["approved_phdr_correction"], pins)
    hold(root, {"path": receipt["baseline_path"], "sha256": receipt["baseline_sha256"]}, pins)
    require(len(review["held_input_pins"]) == review["held_input_pin_count"] == 3095, "independent review pin census changed")
    held_counts = {}
    for row in review["held_input_pins"]:
        relative = str(input_path(root, row["path"]).relative_to(root))
        held_counts[relative] = held_counts.get(relative, 0) + 1
    require(len(held_counts) == 3091 and {path: count for path, count in held_counts.items() if count != 1} ==
            {"modules/kern/src/syscalls_impl/process/exec.cpp": 2, "modules/kern/src/platform/sys/syscall.cpp": 2,
             "modules/kern/src/platform/sys/syscall.asm": 2, "modules/kern/src/abi/callnums.hpp": 2},
            "independent review input census differs from exact historical3095 rows/3091 paths")
    for row in review["held_input_pins"]:
        hold_archived_context(root, row, pins, successor, "tiny_held3095")
    for row in receipt["commands"]:
        require(row["returncode"] == 0 and not row.get("timeout", False) and row["timeout_seconds"] == 30, "raw artifact command did not pass")
        hold(root, row["log"], pins)
    for name in SOURCES:
        require(not (root / name).is_symlink(), f"pilot source is redirected: {name}")
        hold(root, describe(root / name, root), pins)
    target_paths = header_paths(root)
    require(target_paths == sorted(row["path"] for row in receipt["target_include_inputs"]), "target header census differs from raw build")
    kernel_paths = header_paths(root, kernel=True)
    header_snapshot(root, target_paths, pins)
    header_snapshot(root, kernel_paths, pins)
    # WOS's hosted PIE link selects Scrt1.o, libc.so and compiler-rt builtins.
    # Pin the loader as well, because it will execute the hosted parent in WOS.
    for name in ("toolchain/sysroot/lib/Scrt1.o", "toolchain/sysroot/lib/libc.so", "toolchain/sysroot/lib/ld.so",
                 "toolchain/host/lib/clang/22/lib/x86_64-pc-wos/libclang_rt.builtins-x86_64.a",
                 "toolchain/host/lib/clang/22/lib/x86_64-pc-wos/libclang_rt.builtins.a",
                 "toolchain/src/llvm-project/clang/lib/Driver/ToolChains/WOS.cpp"):
        hold(root, describe(root / name, root), pins)
    built = {row["role"]: row for row in receipt["artifacts"]}
    reviewed = {row["role"]: row for row in review["artifacts"]}
    require(len(receipt["artifacts"]) == len(review["artifacts"]) == 3 and set(built) == set(reviewed) == set(ARTIFACTS), "wrong artifact roles")
    blobs = {}
    for role, (label, filename, size, entry, phnum, sha) in ARTIFACTS.items():
        row, independent = built[role], reviewed[role]
        artifact = input_path(root, row["path"])
        require(artifact.name == filename and row["bytes"] == size and row["sha256"] == sha, f"wrong exact artifact: {role}")
        require(independent["independent_binary_measurements_equal"] is True and independent["artifact"]["sha256"] == sha, f"unreviewed artifact: {role}")
        require(row["entry"] == independent["entry"] == entry and row["phnum"] == independent["phnum"] == phnum, f"wrong entry/PHNUM: {role}")
        require(row["structured_elf_gate"] == "PASS" and row["entry_disassembly"]["automatic_entry_disassembly_gate"] == "PASS", f"ELF/disassembly gate failed: {role}")
        require(all(independent["disassembly"][key] is True for key in ("call_and_tail_graph_acyclic", "no_SIMD_or_TLS_references", "genuine_entry_and_raw_source_register_review")), f"independent instruction review failed: {role}")
        hold(root, row, pins)
        data = artifact.read_bytes()
        require(len(data) == size and digest(data) == sha, f"artifact changed while reading: {role}")
        header = struct.unpack_from("<16sHHIQQQIHHHHHH", data)
        require(header[0][:7] == b"\x7fELF\x02\x01\x01" and header[1] == (3 if role == "interpreter" else 2) and header[2:4] == (62, 1), f"wrong real ELF header: {role}")
        require(header[4:6] == (entry, 64) and header[8:11] == (64, 56, phnum), f"real ELF metadata mismatch: {role}")
        blobs[label] = data
    require(sum(map(len, blobs.values())) == 44584, "unexpected total embedded raw bytes")
    require({(row["collection"], row["path"]) for row in successor["historical_context_inputs"]} ==
            {("old617", relative) for relative in CONTEXT_PATHS} |
            {("tiny_baseline51", TINY_CONTEXT), ("tiny_held3095", TINY_CONTEXT)}, "missing exact historical context rows")
    stable(root, pins)
    return pins, blobs, target_paths, kernel_paths, receipt_pin, review_pin, successor


def expected_header(blobs):
    lines = ["// Generated from the immutable, independently reviewed actual raw artifacts.",
             "#pragma once", "#include <array>", "#include <cstdint>", "namespace pilot_artifacts {",
             "inline constexpr uint64_t MAIN_ENTRY = 0x401000;", "inline constexpr uint64_t STATIC_PHNUM = 4;",
             "inline constexpr uint64_t TINY_PHNUM = 5;", "inline constexpr uint64_t INTERP_ENTRY = 0x1000;",
             "inline constexpr uint64_t INTERP_PHNUM = 7;"]
    for label in ("STATIC", "TINY", "INTERP"):
        data = blobs[label]
        lines.append(f'inline constexpr char {label}_SHA256[] = "{digest(data)}";')
        lines.append(f"inline constexpr std::array<unsigned char, {len(data)}> {label}_BYTES{{{{")
        for offset in range(0, len(data), 16):
            lines.append("    " + ", ".join(f"0x{byte:02x}" for byte in data[offset:offset + 16]) + ",")
        lines.append("}};")
    lines.append("}  // namespace pilot_artifacts")
    return ("\n".join(lines) + "\n").encode("ascii")


def bounded_outputs(directory):
    total = 0
    count = 0
    for path in directory.rglob("*"):
        require(not path.is_symlink(), f"output symlink appeared: {path}")
        if path.is_file():
            count += 1
            total += path.stat().st_size
        else:
            require(path.is_dir(), f"nonregular output appeared: {path}")
    require(count <= MAX_OUTPUT_FILES and total <= MAX_DIRECTORY_BYTES, "pilot artifact directory exceeds storage bound")
    return total


def write_new(path, data):
    require(len(data) <= MAX_DIRECTORY_BYTES, f"output exceeds bound: {path}")
    with path.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def save_json(path, value, directory):
    data = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
    require(bounded_outputs(directory) + len(data) <= MAX_DIRECTORY_BYTES, "receipt exceeds remaining storage budget")
    write_new(path, data)


def limit_child_files():
    resource.setrlimit(resource.RLIMIT_FSIZE, (MAX_COMMAND_FILE_BYTES, MAX_COMMAND_FILE_BYTES))


def command(argv, root, output, name, commands):
    bounded_outputs(output)
    log = output / f"{name}.log"
    environment = {"PATH": str(root / "toolchain/host/bin") + ":/usr/bin:/bin", "LANG": "C", "LC_ALL": "C", "TMPDIR": str(output / ".tmp")}
    row = {"name": name, "argv": argv, "cwd": str(root), "environment": environment,
           "timeout_seconds": 30, "child_file_size_limit_bytes": MAX_COMMAND_FILE_BYTES}
    commands.append(row)
    start = time.monotonic()
    try:
        with log.open("xb") as stream:
            with subprocess.Popen(argv, cwd=root, env=environment, stdin=subprocess.DEVNULL, stdout=stream, stderr=subprocess.STDOUT,
                                  start_new_session=True, preexec_fn=limit_child_files) as process:
                row["owned_process_pid"] = process.pid
                try:
                    row["returncode"] = process.wait(timeout=30)
                except BaseException:
                    if process.returncode is None:
                        try:
                            os.killpg(process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        row["returncode"] = process.wait(timeout=5)
                    raise
    except subprocess.TimeoutExpired:
        row["timeout"] = True
        raise
    finally:
        row["elapsed_seconds"] = time.monotonic() - start
        if log.is_file():
            row["log"] = describe(log, root)
        bounded_outputs(output)
    require(row["returncode"] == 0, f"command failed: {name}; see {log}")


def dependency_gate(path, root, output, pins, generated_header):
    data = path.read_text()
    require(len(data) <= MAX_COMMAND_FILE_BYTES and ":" in data, f"invalid dependency output: {path}")
    # Clang/lld make dependency files escape spaces and continue lines with a
    # backslash. No shell parsing or execution is involved.
    # LLD emits empty phony rules after the first complete dependency rule.
    # They are not additional linker inputs.
    source = data.replace("\\\n", " ").splitlines()[0].split(":", 1)[1]
    names, word = [], ""
    escaped = False
    for character in source:
        if escaped:
            word += character
            escaped = False
        elif character == "\\":
            escaped = True
        elif character.isspace():
            if word:
                names.append(word.replace("$$", "$"))
                word = ""
        else:
            word += character
    require(not escaped, "truncated dependency escape")
    if word:
        names.append(word.replace("$$", "$"))
    require(names and len(names) <= MAX_HEADER_FILES, "dependency census outside bound")
    rows = []
    for name in dict.fromkeys(names):
        path = input_path(root, name)
        if path == generated_header:
            rows.append(describe(path, root))
        elif path.is_relative_to(output):
            require(not path.is_symlink() and path.stat().st_size <= MAX_COMMAND_FILE_BYTES, "invalid owned compiler intermediate")
            rows.append(describe(path, root))
        else:
            key = str(path.relative_to(root))
            require(key in pins, f"compiler/linker read an unpinned input: {key}")
            require(describe(path, root) == pins[key], f"dependency changed: {key}")
            rows.append(pins[key])
    return rows


def parent_elf_gate(path):
    data = path.read_bytes()
    require(64 <= len(data) <= MAX_COMMAND_FILE_BYTES, "hosted parent ELF size outside bound")
    header = struct.unpack_from("<16sHHIQQQIHHHHHH", data)
    require(header[0][:7] == b"\x7fELF\x02\x01\x01" and header[1:4] == (3, 62, 1), "parent is not a WOS x86_64 PIE")
    entry, phoff, phentsize, phnum = header[4], header[5], header[9], header[10]
    require(header[8] == 64 and phentsize == 56 and 1 <= phnum <= 32 and phoff + phnum * 56 <= len(data), "invalid parent PHDR table")
    programs = [struct.unpack_from("<IIQQQQQQ", data, phoff + i * 56) for i in range(phnum)]
    interpreters, stacks, dynamic, loads = [], [], [], []
    for program in programs:
        kind, flags, offset, address, _, filesz, memsz, _ = program
        require(offset <= len(data) and filesz <= len(data) - offset and filesz <= memsz, "parent program range outside ELF")
        if kind == 1:
            require(not (flags & 3 == 3), "parent has a writable executable load")
            loads.append(program)
        elif kind == 3:
            require(1 <= filesz <= 512, "parent interpreter string outside bound")
            interpreters.append(data[offset:offset + filesz])
        elif kind == 2:
            dynamic.append(program)
        elif kind == 0x6474E551:
            stacks.append(flags)
    require(interpreters == [b"/lib/ld.so\0"] and len(stacks) == 1 and not stacks[0] & 1, "parent interpreter/NX stack mismatch")
    require(any(p[1] & 1 and p[3] <= entry < p[3] + p[5] for p in loads), "parent entry is not executable file bytes")
    require(len(dynamic) == 1 and dynamic[0][5] % 16 == 0, "invalid parent dynamic table")
    rows = []
    for offset in range(dynamic[0][2], dynamic[0][2] + dynamic[0][5], 16):
        tag, value = struct.unpack_from("<qQ", data, offset)
        rows.append((tag, value))
        if tag == 0:
            break
    require(rows and rows[-1][0] == 0, "unterminated parent dynamic table")
    strings = [value for tag, value in rows if tag == 5]
    sizes = [value for tag, value in rows if tag == 10]
    require(len(strings) == len(sizes) == 1 and 0 < sizes[0] <= MAX_COMMAND_FILE_BYTES, "invalid parent dynamic strings")
    containing = [p for p in loads if p[3] <= strings[0] and strings[0] - p[3] <= p[5] and sizes[0] <= p[5] - (strings[0] - p[3])]
    require(len(containing) == 1, "parent strings outside one file-backed load")
    start = containing[0][2] + strings[0] - containing[0][3]
    table = data[start:start + sizes[0]]
    needed = []
    for tag, value in rows:
        if tag == 1:
            require(value < len(table) and table.find(b"\0", value) >= value, "invalid parent dependency string")
            needed.append(table[value:table.find(b"\0", value)].decode("ascii"))
    require(needed == ["libc.so"], "parent links a dependency beyond libc")
    return {"sha256": digest(data), "bytes": len(data), "elf_type": 3, "entry": entry, "phnum": phnum,
            "interpreter": "/lib/ld.so", "needed": needed, "structured_parent_elf_gate": "PASS"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--artifact-receipt", required=True, type=Path)
    parser.add_argument("--artifact-review", required=True, type=Path)
    parser.add_argument("--source-successor", required=True, type=Path)
    parser.add_argument("--source-successor-sha256", required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    output = Path(os.path.abspath(args.output_dir))
    parent = root / "build/goal39-evidence"
    try:
        require(os.geteuid() != 0, "run the pilot builder as the ordinary repository owner")
        require(parent.is_dir() and parent.resolve(strict=True) == parent, "evidence parent is missing or redirected")
        require(output.parent == parent and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}", output.name), "output must be a named direct child of build/goal39-evidence")
        require(not output.exists() and not output.is_symlink(), f"output collision: {output}")
        pins, blobs, target_paths, kernel_paths, receipt_pin, review_pin, successor = preflight(
            root, args.artifact_receipt, args.artifact_review, args.source_successor, args.source_successor_sha256)
        header_bytes = expected_header(blobs)
        require(len(header_bytes) <= MAX_COMMAND_FILE_BYTES, "generated expectations header exceeds bound")
        # This is the first mutation. mkdir without exist_ok also closes the
        # ordinary collision race; preflight never creates an output directory.
        output.mkdir(mode=0o700)
    except Exception as error:
        print(json.dumps({"result": "FAIL_PREFLIGHT", "error": f"{type(error).__name__}: {error}", "output_directory_created": False}))
        return 1
    receipt = {"kind": "WOS_RAW_LIFECYCLE_PILOT_BUILD", "result": "IN_PROGRESS", "output_directory": str(output),
               "artifact_receipt": receipt_pin, "artifact_review": review_pin, "source_admission": pins[ADMISSION[0]],
               "source_successor": successor["manifest"], "historical_context_inputs": successor["historical_context_inputs"],
               "input_pins": list(pins.values()), "commands": [], "runtime_performed": False,
               "private_startup_qualified": False, "stage_consumption_qualified": False, "controller_census_qualified": False}
    header = output / "lifecycle_raw_pilot_expectations.hpp"
    sysroot = root / "toolchain/sysroot"
    clang = str(root / "toolchain/host/bin/clang++")
    linker = str(root / "toolchain/host/bin/ld.lld")
    objdump = str(root / "toolchain/host/bin/llvm-objdump")
    readobj = str(root / "toolchain/host/bin/llvm-readobj")
    executable = output / "lifecycle-raw-pilot"
    try:
        (output / ".tmp").mkdir(mode=0o700)
        write_new(header, header_bytes)
        receipt["generated_expectations_header"] = describe(header, root)
        receipt["embedded_raw_bytes"] = sum(map(len, blobs.values()))
        receipt["complete_pilot_sources"] = []
        for name in SOURCES:
            data = (root / name).read_bytes()
            require(digest(data) == pins[name]["sha256"], f"pilot source changed: {name}")
            receipt["complete_pilot_sources"].append({**pins[name], "zlib_base64_complete_bytes": base64.b64encode(zlib.compress(data, 9)).decode("ascii")})
        save_json(output / "source-before.json", receipt, output)
        driver = [clang, "--no-default-config", "--target=x86_64-pc-wos", f"--sysroot={sysroot}",
                  "-resource-dir", str(root / "toolchain/host/lib/clang/22")]
        object_file = output / "pilot.o"
        argv = driver + ["-std=c++23", "-O2", "-g", "-fPIE", "-nostdinc", "-nostdinc++",
                "-fno-stack-protector", "-fno-stack-check", "-Wall", "-Wextra", "-Werror", "-fno-exceptions", "-fno-rtti",
                "-fno-sanitize=safe-stack", "-isystem", str(sysroot / "include/c++/v1"),
                "-isystem", str(root / "toolchain/host/lib/clang/22/include"), "-isystem", str(sysroot / "include"),
                "-I", str(root / "modules/kern/src"), "-I", str(output), "-MD", "-MF", str(output / "pilot-compile.d"),
                "-c", str(root / SOURCES[0]), "-o", str(object_file)]
        command(argv, root, output, "compile-pilot", receipt["commands"])
        receipt["compile_dependencies"] = dependency_gate(output / "pilot-compile.d", root, output, pins, header)
        # Keep the object in the owned directory: clang removes its temporary
        # object after a one-shot compile/link, which would erase an actual link
        # input before the dependency gate could verify it.
        argv = driver + [str(object_file), f"-fuse-ld={linker}", "-L" + str(sysroot / "lib"), "-nostdlib++", "--rtlib=compiler-rt",
                "--unwindlib=none", "-lc", "-Wl,-z,noexecstack", "-Wl,--dynamic-linker=/lib/ld.so",
                "-Wl,--dependency-file=" + str(output / "pilot-link.d"),
                "-o", str(executable)]
        command(argv, root, output, "link-pilot", receipt["commands"])
        receipt["link_dependencies"] = dependency_gate(output / "pilot-link.d", root, output, pins, header)
        receipt["parent_artifact"] = {**describe(executable, root), **parent_elf_gate(executable)}
        command([readobj, "--file-headers", "--program-headers", "--dynamic-table", "--relocations", "--symbols", str(executable)],
                root, output, "inspect-pilot", receipt["commands"])
        command([objdump, "-d", "--no-show-raw-insn", str(executable)], root, output, "disassemble-pilot", receipt["commands"])
        require(header.read_bytes() == header_bytes, "generated expected bytes changed during build")
        require(header_paths(root) == target_paths and header_paths(root, kernel=True) == kernel_paths, "target/kernel header census changed")
        stable(root, pins)
        receipt["inputs_stable"] = True
        receipt["result"] = "PASS_ARTIFACT_ONLY"
    except Exception as error:
        receipt["result"] = "FAIL"
        receipt["error"] = f"{type(error).__name__}: {error}"
        receipt["failure_outputs_retained"] = True
    save_json(output / "pilot-artifact-receipt.json", receipt, output)
    output_bytes = bounded_outputs(output)
    print(json.dumps({"result": receipt["result"], "receipt": str(output / "pilot-artifact-receipt.json"),
                      "output_bytes": output_bytes, "runtime_performed": False}))
    return 0 if receipt["result"] == "PASS_ARTIFACT_ONLY" else 1


if __name__ == "__main__":
    raise SystemExit(main())
