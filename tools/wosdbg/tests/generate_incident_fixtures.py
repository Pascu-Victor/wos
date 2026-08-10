#!/usr/bin/env python3
"""Generate deterministic WOS incident bundles used by WOSDBG assurance tests.

The generated corpus is intentionally self-contained: coredumps and ELF files
are synthesized byte-for-byte, tar metadata is fixed, and no build artifact is
consulted.  The fixture index separates loader workflows, hostile-container
cases, and raw collector inputs so backend tests can run before collectors are
implemented (and vice versa).
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import io
import json
import struct
import tarfile
from pathlib import Path, PurePosixPath
from typing import Any, Iterable


FIXTURE_CREATED_UTC = "2026-01-01T00:00:00Z"
FIXTURE_REVISION = "0123456789abcdef0123456789abcdef01234567"
COREDUMP_MAGIC = 0x504D55444F43534F
ELF_LOAD_BASE = 0x400000
PAGE_SIZE = 4096
BUILD_ID_A = bytes.fromhex("00112233445566778899aabbccddeeff00112233")
BUILD_ID_B = bytes.fromhex("ffeeddccbbaa99887766554433221100ffeeddcc")


def canonical_json(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n").encode("utf-8")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def align(value: int, alignment: int) -> int:
    return (value + alignment - 1) & ~(alignment - 1)


def fixed_c_string(value: str, size: int) -> bytes:
    encoded = value.encode("utf-8")
    if len(encoded) >= size:
        raise ValueError(f"fixed string {value!r} does not fit in {size} bytes")
    return encoded + bytes(size - len(encoded))


def minimal_elf(build_id: bytes) -> bytes:
    """Return a deterministic ELF64 image with one symbol and GNU build ID."""

    if not build_id:
        raise ValueError("build ID must not be empty")

    ehdr_size = 64
    phdr_size = 56
    phoff = ehdr_size
    code_offset = align(ehdr_size + phdr_size, 16)
    code = bytes.fromhex("554889e5909090904889ec5dc3909090")

    note_offset = align(code_offset + len(code), 4)
    note = struct.pack("<III", 4, len(build_id), 3) + b"GNU\0"
    note += bytes(align(len(note), 4) - len(note))
    note += build_id
    note += bytes(align(len(note), 4) - len(note))

    shstr = b"\0.shstrtab\0.note.gnu.build-id\0.strtab\0.symtab\0"
    shstr_offset = note_offset + len(note)
    strtab = b"\0fixture_entry\0"
    strtab_offset = shstr_offset + len(shstr)
    symtab_offset = align(strtab_offset + len(strtab), 8)
    null_symbol = bytes(24)
    symbol = struct.pack("<IBBHQQ", 1, 0x12, 0, 1, ELF_LOAD_BASE + code_offset, len(code))
    symtab = null_symbol + symbol
    shoff = align(symtab_offset + len(symtab), 8)
    shnum = 5
    total_size = shoff + shnum * 64

    image = bytearray(total_size)
    ident = b"\x7fELF" + bytes([2, 1, 1, 0]) + bytes(8)
    image[:64] = ident + struct.pack(
        "<HHIQQQIHHHHHH",
        2,
        62,
        1,
        ELF_LOAD_BASE + code_offset,
        phoff,
        shoff,
        0,
        ehdr_size,
        phdr_size,
        1,
        64,
        shnum,
        1,
    )
    image[phoff : phoff + phdr_size] = struct.pack(
        "<IIQQQQQQ", 1, 5, 0, ELF_LOAD_BASE, ELF_LOAD_BASE, total_size, total_size, PAGE_SIZE
    )
    image[code_offset : code_offset + len(code)] = code
    image[note_offset : note_offset + len(note)] = note
    image[shstr_offset : shstr_offset + len(shstr)] = shstr
    image[strtab_offset : strtab_offset + len(strtab)] = strtab
    image[symtab_offset : symtab_offset + len(symtab)] = symtab

    def shdr(
        name: int,
        kind: int,
        flags: int,
        address: int,
        offset: int,
        size: int,
        link: int = 0,
        info: int = 0,
        addralign: int = 1,
        entsize: int = 0,
    ) -> bytes:
        return struct.pack("<IIQQQQIIQQ", name, kind, flags, address, offset, size, link, info, addralign, entsize)

    names = {
        "shstr": shstr.index(b".shstrtab"),
        "note": shstr.index(b".note.gnu.build-id"),
        "strtab": shstr.index(b".strtab"),
        "symtab": shstr.index(b".symtab"),
    }
    sections = [
        bytes(64),
        shdr(names["shstr"], 3, 0, 0, shstr_offset, len(shstr)),
        shdr(names["note"], 7, 2, ELF_LOAD_BASE + note_offset, note_offset, len(note), addralign=4),
        shdr(names["strtab"], 3, 0, 0, strtab_offset, len(strtab)),
        shdr(names["symtab"], 2, 0, 0, symtab_offset, len(symtab), link=3, info=1, addralign=8, entsize=24),
    ]
    for index, section in enumerate(sections):
        image[shoff + index * 64 : shoff + (index + 1) * 64] = section
    return bytes(image)


def elf_with_displaced_chunk(build_id: bytes, chunk_offset: int = PAGE_SIZE) -> tuple[bytes, bytes]:
    """Return a valid ELF plus deterministic bytes displaced by one chunk.

    The ELF's PT_LOAD extent remains the original compact image.  Extra bytes at
    ``chunk_offset`` are nevertheless part of the owned binary buffer, allowing
    WOSDBG's corruption scanner to prove that captured PT_LOAD bytes came from a
    different, chunk-aligned file offset rather than merely differing at random.
    """

    base = minimal_elf(build_id)
    if chunk_offset < len(base):
        raise ValueError("chunk offset must follow the compact ELF image")
    displaced = bytes(((index * 73) + 19) & 0xFF for index in range(len(base)))
    image = base + bytes(chunk_offset - len(base)) + displaced
    if image.find(displaced) != chunk_offset:
        raise AssertionError("displaced corruption pattern is not unique at its intended chunk offset")
    return image, displaced


def build_coredump(
    version: int,
    embedded_elf: bytes,
    *,
    pid: int = 42,
    node_number: int = 0,
    with_segment: bool = True,
    captured_page_prefix: bytes | None = None,
) -> bytes:
    """Build a canonical coredump using the local v1-v3 on-disk layouts."""

    if version == 1:
        header_size = 488
        segment_entry_size = 32
    elif version == 2:
        header_size = 1360
        segment_entry_size = 48
    else:
        # Unsupported-version fixtures deliberately retain the v3 shape.
        header_size = 1840
        segment_entry_size = 48

    segment_count = 1 if with_segment else 0
    segment_table_offset = header_size
    segment_bytes = segment_count * segment_entry_size
    memory_offset = header_size + segment_bytes
    elf_offset = memory_offset + (PAGE_SIZE if with_segment else 0) if embedded_elf else 0
    trap_rip = ELF_LOAD_BASE + 0x80
    trap_rsp = 0x7FFF00001000 + node_number * 0x10000

    trap_frame = (14, 4, trap_rip, 0x23, 0x202, trap_rsp, 0x1B)
    trap_regs = (15, 14, 13, 12, 11, 10, 9, 8, trap_rsp + 0x80, 7, 6, 5, 4, 3, 2)
    saved_frame = (0, 0, trap_rip, 0x23, 0x202, trap_rsp + 0x40, 0x1B)
    saved_regs = (115, 114, 113, 112, 111, 110, 109, 108, trap_rsp + 0xA0, 107, 106, 105, 104, 103, 102)

    header = struct.pack(
        "<QII7Q",
        COREDUMP_MAGIC,
        version,
        header_size,
        1_000_000 + node_number,
        pid,
        node_number,
        14,
        4,
        ELF_LOAD_BASE + 0x123,
        0x1000 + node_number,
    )
    header += struct.pack("<7Q", *trap_frame)
    header += struct.pack("<15Q", *trap_regs)
    header += struct.pack("<7Q", *saved_frame)
    header += struct.pack("<15Q", *saved_regs)
    header += struct.pack(
        "<8Q",
        trap_rip,
        0x2000 + node_number,
        ELF_LOAD_BASE,
        ELF_LOAD_BASE + 64,
        segment_count,
        segment_table_offset,
        len(embedded_elf),
        elf_offset,
    )
    if len(header) != 488:
        raise AssertionError(f"v1 header prefix has unexpected size {len(header)}")

    if version >= 2:
        header += struct.pack(
            "<13Q",
            segment_entry_size,
            PAGE_SIZE,
            2,
            0x70000000,
            1,
            56,
            0x71000000,
            0,
            trap_rsp - PAGE_SIZE,
            PAGE_SIZE * 4,
            0x72000000,
            PAGE_SIZE,
            trap_rsp + 0x200,
        )
        header += fixed_c_string("/bin/fixture-app", 256)
        header += fixed_c_string("/fixture", 256)
        header += fixed_c_string("/", 256)
    if version >= 3:
        header += fixed_c_string("fixture-wait", 64)
        header += fixed_c_string(f"wos-{node_number}", 64)
        header += fixed_c_string("collector", 64)
        rich_task_values = (
            0xFFFF800000001000,
            0xFFFF800000002000,
            1,
            pid,
            9000 + node_number,
            77,
            42,
            1,
            2,
            3,
            node_number,
            node_number,
            1 << node_number,
            0,
            0,
            ELF_LOAD_BASE,
            len(embedded_elf),
            0,
            100,
            20,
            10,
            30,
            40,
            50,
            0x1234,
            0,
            0,
            0,
            0,
            0,
            0,
            1000,
            1000,
            1000,
            1000,
            4,
        )
        if len(rich_task_values) != 36:
            raise AssertionError("v3 rich task fixture must contain exactly 36 uint64 values")
        header += struct.pack("<36Q", *rich_task_values)
    if len(header) != header_size:
        raise AssertionError(f"v{version} header has size {len(header)}, expected {header_size}")

    segment_table = b""
    page = b""
    if with_segment:
        segment_table = struct.pack("<QQQII", ELF_LOAD_BASE, PAGE_SIZE, memory_offset, 3, 1)
        if segment_entry_size == 48:
            segment_table += struct.pack("<QQ", 0x5, 0x3000 + node_number * PAGE_SIZE)
        page_data = bytearray((index + node_number) & 0xFF for index in range(PAGE_SIZE))
        page_data[: min(len(embedded_elf), PAGE_SIZE)] = embedded_elf[:PAGE_SIZE]
        if captured_page_prefix is not None:
            page_data[: min(len(captured_page_prefix), PAGE_SIZE)] = captured_page_prefix[:PAGE_SIZE]
        page = bytes(page_data)
    return header + segment_table + page + embedded_elf


@dataclasses.dataclass(frozen=True)
class Artifact:
    path: str
    kind: str
    data: bytes
    required: bool = True
    truncated: bool = False
    node_id: int | str | None = None
    build_id: str | None = None
    binary: str | None = None
    source_name: str | None = None
    clock_domain: str | None = None
    sha256_override: str | None = None
    size_override: int | None = None

    def member(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "path": self.path,
            "kind": self.kind,
            "size": len(self.data) if self.size_override is None else self.size_override,
            "sha256": sha256(self.data) if self.sha256_override is None else self.sha256_override,
            "required": self.required,
            "truncated": self.truncated,
            "sourceName": self.source_name or PurePosixPath(self.path).name,
        }
        if self.node_id is not None:
            result["nodeId"] = self.node_id
        if self.build_id is not None:
            result["buildId"] = self.build_id
        if self.binary is not None:
            result["binary"] = self.binary
        if self.clock_domain is not None:
            result["clockDomain"] = self.clock_domain
        return result


def validate_member_path(path: str) -> None:
    pure = PurePosixPath(path)
    if pure.is_absolute() or not pure.parts or any(part in {"", ".", ".."} for part in pure.parts):
        raise ValueError(f"unsafe generated member path: {path!r}")


def make_manifest(
    fixture_name: str,
    artifacts: Iterable[Artifact],
    *,
    nodes: list[dict[str, Any]],
    clock_quality: str,
    clock_domains: list[dict[str, Any]],
    source_kind: str = "ktest",
    profile: str = "fixture",
    complete: bool = True,
    errors: list[dict[str, Any]] | None = None,
    redactions: int = 0,
) -> dict[str, Any]:
    if not fixture_name:
        raise ValueError("fixture name must not be empty")
    ordered = sorted(artifacts, key=lambda artifact: artifact.path)
    config_members = [artifact.path for artifact in ordered if artifact.kind == "config"]
    if len(config_members) != 1:
        raise ValueError("each fixture bundle must contain exactly one config member")
    truncated_members = [artifact.path for artifact in ordered if artifact.truncated]
    manifest: dict[str, Any] = {
        "format": "wosincident",
        "version": 1,
        "incidentId": "",
        "createdUtc": FIXTURE_CREATED_UTC,
        "source": {
            "kind": source_kind,
            "revision": FIXTURE_REVISION,
            "dirty": False,
            "profile": profile,
            "configMember": config_members[0],
        },
        "capture": {
            "complete": complete,
            "errors": sorted(
                list(errors or []),
                key=lambda error: (
                    str(error.get("code", "")),
                    str(error.get("sourceName", "")),
                    str(error.get("message", "")),
                    bool(error.get("required", False)),
                ),
            ),
            "redactions": redactions,
            "truncatedMembers": truncated_members,
        },
        "clocks": {"quality": clock_quality, "domains": clock_domains},
        "topology": {"nodes": nodes},
        "members": [artifact.member() for artifact in ordered],
    }
    identity = dict(manifest)
    identity.pop("incidentId")
    identity.pop("createdUtc")
    identity_bytes = json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    manifest["incidentId"] = "sha256:" + sha256(identity_bytes)
    return manifest


def write_directory_bundle(root: Path, name: str, manifest: dict[str, Any], artifacts: Iterable[Artifact]) -> Path:
    bundle = root / "bundles" / f"{name}.wosincident"
    bundle.mkdir(parents=True, exist_ok=True)
    for artifact in sorted(artifacts, key=lambda item: item.path):
        validate_member_path(artifact.path)
        target = bundle.joinpath(*PurePosixPath(artifact.path).parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(artifact.data)
    (bundle / "manifest.json").write_bytes(canonical_json(manifest))
    return bundle


def tar_info(path: str, size: int, *, kind: bytes = tarfile.REGTYPE, linkname: str = "") -> tarfile.TarInfo:
    info = tarfile.TarInfo(path)
    info.size = size
    info.mode = 0o644
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mtime = 0
    info.type = kind
    info.linkname = linkname
    return info


def archive_bytes(manifest: dict[str, Any], artifacts: Iterable[Artifact]) -> bytes:
    output = io.BytesIO()
    entries = [("manifest.json", canonical_json(manifest))]
    entries.extend((artifact.path, artifact.data) for artifact in artifacts)
    with tarfile.open(fileobj=output, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for path, data in sorted(entries):
            archive.addfile(tar_info(path, len(data)), io.BytesIO(data))
    return output.getvalue()


def write_archive_bundle(root: Path, name: str, manifest: dict[str, Any], artifacts: Iterable[Artifact]) -> Path:
    target = root / "archives" / f"{name}.wosincident"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(archive_bytes(manifest, artifacts))
    return target


def raw_tar(entries: Iterable[tuple[tarfile.TarInfo, bytes | None]]) -> bytes:
    """Construct a deterministic tar, including intentionally malformed entries."""

    output = bytearray()
    for info, data in entries:
        output += info.tobuf(format=tarfile.USTAR_FORMAT, encoding="utf-8", errors="strict")
        if data is not None:
            output += data
            output += bytes((-len(data)) % 512)
    output += bytes(1024)
    return bytes(output)


def node(node_id: int, hostname: str) -> dict[str, Any]:
    return {"debug": False, "hostname": hostname, "id": node_id, "nics": []}


def clock_domain(domain_id: str, node_ids: list[int], comparable: bool) -> dict[str, Any]:
    result: dict[str, Any] = {
        "comparableAcrossNodes": comparable,
        "id": domain_id,
        "source": "fixture-synchronized" if comparable else "guest-boot-monotonic",
        "synchronized": comparable,
    }
    if len(node_ids) == 1:
        result["nodeId"] = node_ids[0]
    else:
        # Multi-node synchronized domains are an additive reader fixture.  The
        # production collector currently emits one nodeId per unsynchronized
        # domain, but the manifest envelope permits a shared domain.
        result["nodeIds"] = node_ids
    return result


def capture_error(code: str, source_name: str, message: str, *, required: bool = True) -> dict[str, Any]:
    return {"code": code, "message": message, "required": required, "sourceName": source_name}


def config_bytes(nodes: list[dict[str, Any]]) -> bytes:
    return canonical_json({"fixture": True, "nodes": nodes})


def log_bytes(node_id: str, count: int = 3, *, timestamps: str = "all", final_newline: bool = True) -> bytes:
    lines = []
    for index in range(count):
        prefix = ""
        if timestamps == "all" or (timestamps == "partial" and index % 2 == 0):
            prefix = f"timestamp={1_000 + index * 10}ns "
        lines.append(f"{prefix}WKI request request_id=req-{index % 3} cookie=cookie-{index % 2} node={node_id}")
    text = "\n".join(lines)
    if final_newline:
        text += "\n"
    return text.encode("utf-8")


def base_artifacts(
    versions: list[int],
    *,
    nodes: list[dict[str, Any]],
    elf: bytes | None = None,
    binary_elf: bytes | None = None,
    include_binary: bool = True,
    with_segment: bool = True,
    timestamps: dict[int, str] | None = None,
    log_count: int = 3,
    log_final_newline: bool = True,
    log_truncated: bool = False,
    log_clock_domains: dict[int, str] | None = None,
    captured_page_prefix: bytes | None = None,
) -> list[Artifact]:
    embedded = minimal_elf(BUILD_ID_A) if elf is None else elf
    local_binary = embedded if binary_elf is None else binary_elf
    result = [Artifact("metadata/config.json", "config", config_bytes(nodes))]
    binary_path = "artifacts/binaries/fixture-app.elf"
    if include_binary:
        result.append(Artifact(binary_path, "binary", local_binary, build_id=sha_build_id(local_binary)))
    for index, version in enumerate(versions):
        node_info = nodes[min(index, len(nodes) - 1)]
        node_id = int(node_info["id"])
        dump_name = f"artifacts/coredumps/{node_id}-v{version}.bin"
        dump = build_coredump(
            version,
            embedded,
            pid=42 + index,
            node_number=index,
            with_segment=with_segment,
            captured_page_prefix=captured_page_prefix,
        )
        result.append(
            Artifact(
                dump_name,
                "coredump",
                dump,
                node_id=node_id,
                build_id=sha_build_id(embedded) if embedded else None,
                binary=binary_path if include_binary else None,
            )
        )
    for node_info in nodes:
        node_id = int(node_info["id"])
        mode = (timestamps or {}).get(node_id, "all")
        result.append(
            Artifact(
                f"artifacts/logs/{node_id}.log",
                "serial-log",
                log_bytes(node_id, log_count, timestamps=mode, final_newline=log_final_newline),
                node_id=node_id,
                truncated=log_truncated,
                clock_domain=(log_clock_domains or {}).get(node_id, f"fixture-global-node{node_id}"),
            )
        )
    return result


def sha_build_id(elf: bytes) -> str:
    marker = b"GNU\0"
    position = elf.find(marker)
    if position < 12:
        return ""
    namesz, descsz, note_type = struct.unpack_from("<III", elf, position - 12)
    if namesz != 4 or note_type != 3:
        return ""
    desc_offset = align(position + namesz, 4)
    return elf[desc_offset : desc_offset + descsz].hex()


def case_record(
    name: str,
    path: Path,
    root: Path,
    *,
    workflow: str,
    valid: bool | None,
    degraded: bool | None,
    issue_codes: list[str],
    clock_quality: str | None = None,
    coredump_versions: list[int] | None = None,
    max_events: int | None = None,
    timeline_clock_quality: str | None = None,
    inventory_page: tuple[int, int] | None = None,
    max_coredumps: int | None = None,
    max_issues: int | None = None,
    max_serialized_bytes: int | None = None,
    min_chunk_corruption_hits: int | None = None,
    chunk_size: int | None = None,
    global_order_available: bool | None = None,
    references_complete: bool | None = None,
    build_id_checks: dict[str, str] | None = None,
    build_ids: dict[str, str] | None = None,
    binary_quarantined: bool | None = None,
    issue_count: int | None = None,
    issues_returned: int | None = None,
    issues_truncated: bool | None = None,
    summary_degraded: bool | None = None,
    summary_issue_codes: list[str] | None = None,
) -> dict[str, Any]:
    expected: dict[str, Any] = {
        "valid": valid,
        "degraded": degraded,
        "issueCodes": issue_codes,
        "semanticDigestStable": workflow == "full",
    }
    if clock_quality is not None:
        expected["clockQuality"] = clock_quality
    if coredump_versions is not None:
        expected["coredumpVersions"] = coredump_versions
    if max_events is not None:
        expected["maxEvents"] = max_events
    if timeline_clock_quality is not None:
        expected["timelineClockQuality"] = timeline_clock_quality
    if inventory_page is not None:
        expected["inventoryPage"] = {"start": inventory_page[0], "count": inventory_page[1]}
    if max_coredumps is not None:
        expected["maxCoredumps"] = max_coredumps
    if max_issues is not None:
        expected["maxIssues"] = max_issues
    if max_serialized_bytes is not None:
        expected["maxSerializedBytes"] = max_serialized_bytes
    if min_chunk_corruption_hits is not None:
        expected["minChunkCorruptionHits"] = min_chunk_corruption_hits
    if chunk_size is not None:
        expected["chunkSize"] = chunk_size
    if global_order_available is not None:
        expected["globalOrderAvailable"] = global_order_available
    if references_complete is not None:
        expected["referencesComplete"] = references_complete
    if build_id_checks is not None:
        expected["buildIdChecks"] = build_id_checks
    if build_ids is not None:
        expected["buildIds"] = build_ids
    if binary_quarantined is not None:
        expected["binaryQuarantined"] = binary_quarantined
    if issue_count is not None:
        expected["issueCount"] = issue_count
    if issues_returned is not None:
        expected["issuesReturned"] = issues_returned
    if issues_truncated is not None:
        expected["issuesTruncated"] = issues_truncated
    if summary_degraded is not None:
        expected["summaryDegraded"] = summary_degraded
    if summary_issue_codes is not None:
        expected["summaryIssueCodes"] = summary_issue_codes
    return {
        "name": name,
        "path": path.relative_to(root).as_posix(),
        "container": "directory" if path.is_dir() else "archive",
        "workflow": workflow,
        "expected": expected,
    }


def generate_loader_cases(root: Path) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    one_node = [node(0, "wos-0")]
    two_nodes = [node(0, "wos-0"), node(1, "wos-1")]
    complete_clock = [clock_domain("fixture-global-node0", [0], True)]

    for version, container in ((1, "directory"), (2, "archive"), (3, "directory")):
        name = f"valid-single-v{version}"
        artifacts = base_artifacts([version], nodes=one_node)
        manifest = make_manifest(
            name,
            artifacts,
            nodes=one_node,
            clock_quality="complete",
            clock_domains=complete_clock,
        )
        path = (
            write_directory_bundle(root, name, manifest, artifacts)
            if container == "directory"
            else write_archive_bundle(root, name, manifest, artifacts)
        )
        cases.append(
            case_record(
                name,
                path,
                root,
                workflow="full",
                valid=True,
                degraded=False,
                issue_codes=[],
                clock_quality="complete",
                coredump_versions=[version],
                timeline_clock_quality="single-node-comparable",
            )
        )

    archive_name = "valid-single-v3-archive"
    archive_artifacts = base_artifacts([3], nodes=one_node)
    archive_manifest = make_manifest(
        archive_name,
        archive_artifacts,
        nodes=one_node,
        clock_quality="complete",
        clock_domains=complete_clock,
    )
    archive_path = write_archive_bundle(root, archive_name, archive_manifest, archive_artifacts)
    cases.append(
        case_record(
            archive_name,
            archive_path,
            root,
            workflow="full",
            valid=True,
            degraded=False,
            issue_codes=[],
            clock_quality="complete",
            coredump_versions=[3],
            timeline_clock_quality="single-node-comparable",
        )
    )

    partial_name = "distributed-partial-clocks"
    partial_artifacts = base_artifacts(
        [3, 3],
        nodes=two_nodes,
        timestamps={0: "all", 1: "partial"},
        log_clock_domains={0: "fixture-global-node0", 1: "node1-local"},
    )
    partial_manifest = make_manifest(
        partial_name,
        partial_artifacts,
        nodes=two_nodes,
        clock_quality="partial",
        clock_domains=[clock_domain("fixture-global-node0", [0], True), clock_domain("node1-local", [1], False)],
        source_kind="cluster",
        profile="cluster-fixture",
    )
    partial_path = write_directory_bundle(root, partial_name, partial_manifest, partial_artifacts)
    cases.append(
        case_record(
            partial_name,
            partial_path,
            root,
            workflow="full",
            valid=True,
            degraded=True,
            issue_codes=["partial_clocks"],
            clock_quality="partial",
            coredump_versions=[3, 3],
            timeline_clock_quality="partial-timestamps-untrusted",
        )
    )

    non_comparable_name = "distributed-non-comparable-clocks"
    non_comparable_artifacts = base_artifacts(
        [3, 3],
        nodes=two_nodes,
        timestamps={0: "none", 1: "none"},
        log_clock_domains={0: "node0-local", 1: "node1-local"},
    )
    non_comparable_manifest = make_manifest(
        non_comparable_name,
        non_comparable_artifacts,
        nodes=two_nodes,
        clock_quality="non-comparable",
        clock_domains=[clock_domain("node0-local", [0], False), clock_domain("node1-local", [1], False)],
        source_kind="cluster",
        profile="cluster-fixture",
    )
    non_comparable_path = write_archive_bundle(root, non_comparable_name, non_comparable_manifest, non_comparable_artifacts)
    cases.append(
        case_record(
            non_comparable_name,
            non_comparable_path,
            root,
            workflow="full",
            valid=True,
            degraded=True,
            issue_codes=["non_comparable_clocks"],
            clock_quality="non-comparable",
            coredump_versions=[3, 3],
            timeline_clock_quality="per-log-order-only",
        )
    )

    missing_domain_name = "distributed-missing-clock-domain"
    missing_domain_artifacts = [
        dataclasses.replace(artifact, clock_domain=None)
        if artifact.kind == "serial-log" and artifact.node_id == 1
        else artifact
        for artifact in base_artifacts(
            [3, 3],
            nodes=two_nodes,
            log_clock_domains={0: "fixture-global", 1: "fixture-global"},
        )
    ]
    missing_domain_manifest = make_manifest(
        missing_domain_name,
        missing_domain_artifacts,
        nodes=two_nodes,
        clock_quality="complete",
        clock_domains=[clock_domain("fixture-global", [0, 1], True)],
        source_kind="cluster",
        profile="cluster-fixture",
    )
    missing_domain_path = write_directory_bundle(
        root, missing_domain_name, missing_domain_manifest, missing_domain_artifacts
    )
    cases.append(
        case_record(
            missing_domain_name,
            missing_domain_path,
            root,
            workflow="full",
            valid=True,
            degraded=True,
            issue_codes=["manifest_log_clock_domain_missing"],
            clock_quality="complete",
            coredump_versions=[3, 3],
            timeline_clock_quality="partial-timestamps-untrusted",
            global_order_available=False,
            references_complete=False,
        )
    )

    unknown_domain_name = "distributed-unknown-clock-domain"
    unknown_domain_artifacts = base_artifacts(
        [3, 3],
        nodes=two_nodes,
        log_clock_domains={0: "fixture-global", 1: "absent-domain"},
    )
    unknown_domain_manifest = make_manifest(
        unknown_domain_name,
        unknown_domain_artifacts,
        nodes=two_nodes,
        clock_quality="complete",
        clock_domains=[clock_domain("fixture-global", [0, 1], True)],
        source_kind="cluster",
        profile="cluster-fixture",
    )
    unknown_domain_path = write_archive_bundle(
        root, unknown_domain_name, unknown_domain_manifest, unknown_domain_artifacts
    )
    cases.append(
        case_record(
            unknown_domain_name,
            unknown_domain_path,
            root,
            workflow="validate-invalid",
            valid=False,
            degraded=True,
            issue_codes=["manifest_log_clock_domain_reference_invalid"],
            coredump_versions=[3, 3],
        )
    )

    unknown_node_name = "distributed-unknown-log-node"
    unknown_node_artifacts = [
        dataclasses.replace(artifact, node_id=99, clock_domain=None)
        if artifact.kind == "serial-log" and artifact.node_id == 1
        else artifact
        for artifact in base_artifacts(
            [3, 3],
            nodes=two_nodes,
            log_clock_domains={0: "fixture-global", 1: "fixture-global"},
        )
    ]
    unknown_node_manifest = make_manifest(
        unknown_node_name,
        unknown_node_artifacts,
        nodes=two_nodes,
        clock_quality="complete",
        clock_domains=[clock_domain("fixture-global", [0, 1], True)],
        source_kind="cluster",
        profile="cluster-fixture",
    )
    unknown_node_path = write_directory_bundle(
        root, unknown_node_name, unknown_node_manifest, unknown_node_artifacts
    )
    cases.append(
        case_record(
            unknown_node_name,
            unknown_node_path,
            root,
            workflow="validate-invalid",
            valid=False,
            degraded=True,
            issue_codes=["manifest_log_clock_domain_missing", "manifest_member_node_reference_invalid"],
            coredump_versions=[3, 3],
        )
    )

    missing_symbols_name = "missing-symbols"
    missing_symbols_artifacts = base_artifacts(
        [3], nodes=one_node, elf=b"", include_binary=False, with_segment=True
    )
    missing_symbols_manifest = make_manifest(
        missing_symbols_name,
        missing_symbols_artifacts,
        nodes=one_node,
        clock_quality="complete",
        clock_domains=complete_clock,
    )
    missing_symbols_path = write_directory_bundle(root, missing_symbols_name, missing_symbols_manifest, missing_symbols_artifacts)
    cases.append(
        case_record(
            missing_symbols_name,
            missing_symbols_path,
            root,
            workflow="full",
            valid=True,
            degraded=True,
            issue_codes=["missing_symbols"],
            clock_quality="complete",
            coredump_versions=[3],
            timeline_clock_quality="single-node-comparable",
        )
    )

    missing_mapping_name = "missing-mapping"
    missing_mapping_artifacts = base_artifacts([3], nodes=one_node, include_binary=True, with_segment=False)
    missing_mapping_manifest = make_manifest(
        missing_mapping_name,
        missing_mapping_artifacts,
        nodes=one_node,
        clock_quality="complete",
        clock_domains=complete_clock,
    )
    missing_mapping_path = write_directory_bundle(root, missing_mapping_name, missing_mapping_manifest, missing_mapping_artifacts)
    cases.append(
        case_record(
            missing_mapping_name,
            missing_mapping_path,
            root,
            workflow="full",
            valid=True,
            degraded=True,
            issue_codes=["missing_mapping"],
            clock_quality="complete",
            coredump_versions=[3],
            timeline_clock_quality="single-node-comparable",
        )
    )

    mismatch_name = "build-id-mismatch"
    mismatch_artifacts = base_artifacts(
        [3], nodes=one_node, binary_elf=minimal_elf(BUILD_ID_B), include_binary=True
    )
    mismatch_manifest = make_manifest(
        mismatch_name,
        mismatch_artifacts,
        nodes=one_node,
        clock_quality="complete",
        clock_domains=complete_clock,
    )
    mismatch_path = write_archive_bundle(root, mismatch_name, mismatch_manifest, mismatch_artifacts)
    cases.append(
        case_record(
            mismatch_name,
            mismatch_path,
            root,
            workflow="full",
            valid=True,
            degraded=True,
            issue_codes=[
                "build_id_mismatch",
                "declared_binary_build_id_mismatch",
                "embedded_binary_build_id_mismatch",
            ],
            clock_quality="complete",
            coredump_versions=[3],
            timeline_clock_quality="single-node-comparable",
            build_id_checks={
                "declaredEmbedded": "compatible",
                "declaredBinary": "mismatch",
                "embeddedBinary": "mismatch",
            },
            build_ids={
                "declaredBuildId": BUILD_ID_A.hex(),
                "embeddedBuildId": BUILD_ID_A.hex(),
                "binaryBuildId": BUILD_ID_B.hex(),
            },
            binary_quarantined=True,
        )
    )

    declared_mismatch_name = "declared-embedded-build-id-mismatch"
    declared_mismatch_artifacts = [
        dataclasses.replace(artifact, build_id=BUILD_ID_B.hex()) if artifact.kind == "coredump" else artifact
        for artifact in base_artifacts([3], nodes=one_node, include_binary=False)
    ]
    declared_mismatch_manifest = make_manifest(
        declared_mismatch_name,
        declared_mismatch_artifacts,
        nodes=one_node,
        clock_quality="complete",
        clock_domains=complete_clock,
    )
    declared_mismatch_path = write_directory_bundle(
        root, declared_mismatch_name, declared_mismatch_manifest, declared_mismatch_artifacts
    )
    cases.append(
        case_record(
            declared_mismatch_name,
            declared_mismatch_path,
            root,
            workflow="full",
            valid=True,
            degraded=True,
            issue_codes=["build_id_mismatch", "declared_embedded_build_id_mismatch"],
            clock_quality="complete",
            coredump_versions=[3],
            timeline_clock_quality="single-node-comparable",
            build_id_checks={
                "declaredEmbedded": "mismatch",
                "declaredBinary": "compatible",
                "embeddedBinary": "compatible",
            },
            build_ids={
                "declaredBuildId": BUILD_ID_B.hex(),
                "embeddedBuildId": BUILD_ID_A.hex(),
                "binaryBuildId": "",
            },
            binary_quarantined=True,
        )
    )

    chunk_name = "chunk-corruption"
    chunk_elf, displaced_chunk = elf_with_displaced_chunk(BUILD_ID_A)
    chunk_artifacts = base_artifacts(
        [3],
        nodes=one_node,
        elf=chunk_elf,
        binary_elf=chunk_elf,
        captured_page_prefix=displaced_chunk,
    )
    chunk_manifest = make_manifest(
        chunk_name,
        chunk_artifacts,
        nodes=one_node,
        clock_quality="complete",
        clock_domains=complete_clock,
    )
    chunk_path = write_archive_bundle(root, chunk_name, chunk_manifest, chunk_artifacts)
    cases.append(
        case_record(
            chunk_name,
            chunk_path,
            root,
            workflow="full",
            valid=True,
            degraded=False,
            issue_codes=[],
            summary_degraded=True,
            summary_issue_codes=["corrupt_chunk"],
            clock_quality="complete",
            coredump_versions=[3],
            timeline_clock_quality="single-node-comparable",
            min_chunk_corruption_hits=1,
            chunk_size=PAGE_SIZE,
        )
    )

    issue_budget_name = "issue-budget-overrun"
    issue_budget_elf, issue_budget_displaced = elf_with_displaced_chunk(BUILD_ID_A)
    issue_budget_artifacts = [
        dataclasses.replace(artifact, clock_domain=None)
        if artifact.kind == "serial-log" and artifact.node_id == 1
        else artifact
        for artifact in base_artifacts(
            [3, 3],
            nodes=two_nodes,
            elf=issue_budget_elf,
            binary_elf=issue_budget_elf,
            log_clock_domains={0: "fixture-global", 1: "fixture-global"},
            captured_page_prefix=issue_budget_displaced,
        )
    ]
    issue_budget_artifacts.append(
        Artifact("artifacts/binaries/no-build-id.elf", "binary", b"not-an-elf\n", build_id="")
    )
    issue_budget_manifest = make_manifest(
        issue_budget_name,
        issue_budget_artifacts,
        nodes=two_nodes,
        clock_quality="complete",
        clock_domains=[clock_domain("fixture-global", [0, 1], True)],
        source_kind="cluster",
        profile="cluster-fixture",
    )
    issue_budget_path = write_archive_bundle(
        root, issue_budget_name, issue_budget_manifest, issue_budget_artifacts
    )
    cases.append(
        case_record(
            issue_budget_name,
            issue_budget_path,
            root,
            workflow="full",
            valid=True,
            degraded=True,
            issue_codes=["binary_build_id_missing", "manifest_log_clock_domain_missing"],
            clock_quality="complete",
            coredump_versions=[3, 3],
            timeline_clock_quality="partial-timestamps-untrusted",
            max_issues=2,
            min_chunk_corruption_hits=2,
            chunk_size=PAGE_SIZE,
            global_order_available=False,
            references_complete=False,
            issue_count=4,
            issues_returned=2,
            issues_truncated=True,
        )
    )

    unsupported_name = "unsupported-coredump-v4"
    unsupported_artifacts = base_artifacts([4], nodes=one_node)
    unsupported_manifest = make_manifest(
        unsupported_name,
        unsupported_artifacts,
        nodes=one_node,
        clock_quality="complete",
        clock_domains=complete_clock,
    )
    unsupported_path = write_directory_bundle(root, unsupported_name, unsupported_manifest, unsupported_artifacts)
    cases.append(
        case_record(
            unsupported_name,
            unsupported_path,
            root,
            workflow="validate-invalid",
            valid=False,
            degraded=True,
            issue_codes=["unsupported_coredump_version"],
            coredump_versions=[4],
        )
    )

    corrupt_name = "corrupt-coredump"
    corrupt_artifacts = base_artifacts([3], nodes=one_node)
    corrupt_list = []
    for artifact in corrupt_artifacts:
        if artifact.kind == "coredump":
            damaged = bytearray(artifact.data)
            damaged[:8] = b"BADCOR!!"
            artifact = dataclasses.replace(artifact, data=bytes(damaged))
        corrupt_list.append(artifact)
    corrupt_manifest = make_manifest(
        corrupt_name,
        corrupt_list,
        nodes=one_node,
        clock_quality="complete",
        clock_domains=complete_clock,
    )
    corrupt_path = write_archive_bundle(root, corrupt_name, corrupt_manifest, corrupt_list)
    cases.append(
        case_record(
            corrupt_name,
            corrupt_path,
            root,
            workflow="validate-invalid",
            valid=False,
            degraded=True,
            issue_codes=["corrupt_coredump"],
            coredump_versions=[3],
        )
    )

    truncated_dump_name = "truncated-coredump"
    truncated_dump_artifacts = base_artifacts([3], nodes=one_node)
    truncated_dump_list = []
    for artifact in truncated_dump_artifacts:
        if artifact.kind == "coredump":
            artifact = dataclasses.replace(artifact, data=artifact.data[:600], truncated=True)
        truncated_dump_list.append(artifact)
    truncated_dump_manifest = make_manifest(
        truncated_dump_name,
        truncated_dump_list,
        nodes=one_node,
        clock_quality="complete",
        clock_domains=complete_clock,
        complete=False,
        errors=[
            capture_error(
                "fixture-truncated",
                "0-v3.bin",
                "coredump capture ended before the advertised v3 header",
            )
        ],
    )
    truncated_dump_path = write_directory_bundle(
        root, truncated_dump_name, truncated_dump_manifest, truncated_dump_list
    )
    cases.append(
        case_record(
            truncated_dump_name,
            truncated_dump_path,
            root,
            workflow="validate-invalid",
            valid=False,
            degraded=True,
            issue_codes=["truncated_coredump"],
            coredump_versions=[3],
        )
    )

    truncated_log_name = "truncated-log"
    truncated_log_artifacts = base_artifacts(
        [3], nodes=one_node, log_final_newline=False, log_truncated=True
    )
    truncated_log_manifest = make_manifest(
        truncated_log_name,
        truncated_log_artifacts,
        nodes=one_node,
        clock_quality="partial",
        clock_domains=complete_clock,
        complete=False,
        errors=[
            capture_error(
                "fixture-truncated",
                "0.log",
                "serial log ended without a final record terminator",
            )
        ],
    )
    truncated_log_path = write_archive_bundle(root, truncated_log_name, truncated_log_manifest, truncated_log_artifacts)
    cases.append(
        case_record(
            truncated_log_name,
            truncated_log_path,
            root,
            workflow="full",
            valid=True,
            degraded=True,
            issue_codes=["partial_clocks", "truncated_log"],
            clock_quality="partial",
            coredump_versions=[3],
            timeline_clock_quality="partial-timestamps-untrusted",
        )
    )

    checksum_name = "checksum-failure"
    checksum_artifacts = base_artifacts([3], nodes=one_node)
    checksum_list = []
    changed = False
    for artifact in checksum_artifacts:
        if not changed and artifact.kind == "serial-log":
            artifact = dataclasses.replace(artifact, sha256_override="0" * 64)
            changed = True
        checksum_list.append(artifact)
    checksum_manifest = make_manifest(
        checksum_name,
        checksum_list,
        nodes=one_node,
        clock_quality="complete",
        clock_domains=complete_clock,
    )
    checksum_path = write_directory_bundle(root, checksum_name, checksum_manifest, checksum_list)
    cases.append(
        case_record(
            checksum_name,
            checksum_path,
            root,
            workflow="load-reject",
            valid=False,
            degraded=None,
            issue_codes=["member_checksum_mismatch"],
        )
    )

    bounds_name = "response-bounds"
    bounds_artifacts = base_artifacts(
        [3, 3],
        nodes=two_nodes,
        log_count=40,
        log_clock_domains={0: "fixture-global", 1: "fixture-global"},
    )
    bounds_manifest = make_manifest(
        bounds_name,
        bounds_artifacts,
        nodes=two_nodes,
        clock_quality="complete",
        clock_domains=[clock_domain("fixture-global", [0, 1], True)],
        source_kind="cluster",
        profile="cluster-fixture",
    )
    bounds_path = write_directory_bundle(root, bounds_name, bounds_manifest, bounds_artifacts)
    cases.append(
        case_record(
            bounds_name,
            bounds_path,
            root,
            workflow="full",
            valid=True,
            degraded=False,
            issue_codes=[],
            clock_quality="complete",
            coredump_versions=[3, 3],
            max_events=8,
            timeline_clock_quality="globally-comparable",
            inventory_page=(1, 2),
            max_coredumps=1,
            max_issues=1,
            max_serialized_bytes=128 * 1024,
        )
    )
    return cases


def security_manifest(name: str) -> tuple[dict[str, Any], list[Artifact]]:
    nodes = [node(0, "wos-0")]
    artifacts = [Artifact("metadata/config.json", "config", config_bytes(nodes))]
    manifest = make_manifest(
        name,
        artifacts,
        nodes=nodes,
        clock_quality="unavailable",
        clock_domains=[],
        complete=False,
        errors=[capture_error("fixture-hostile", name, "hostile fixture")],
    )
    return manifest, artifacts


def generate_security_cases(root: Path) -> list[dict[str, Any]]:
    target_dir = root / "security"
    target_dir.mkdir(parents=True, exist_ok=True)
    cases: list[dict[str, Any]] = []

    for name, target, is_directory in (
        ("source-directory-symlink", "../bundles/valid-single-v3.wosincident", True),
        ("source-archive-symlink", "../archives/valid-single-v3-archive.wosincident", False),
    ):
        path = target_dir / f"{name}.wosincident"
        path.symlink_to(target, target_is_directory=is_directory)
        cases.append(
            case_record(
                name,
                path,
                root,
                workflow="load-reject",
                valid=False,
                degraded=None,
                issue_codes=["source_symlink_rejected"],
            )
        )

    def add_raw(name: str, unsafe_info: tarfile.TarInfo, data: bytes | None, issue: str) -> None:
        manifest, artifacts = security_manifest(name)
        entries: list[tuple[tarfile.TarInfo, bytes | None]] = [
            (tar_info("manifest.json", len(canonical_json(manifest))), canonical_json(manifest))
        ]
        entries.extend((tar_info(artifact.path, len(artifact.data)), artifact.data) for artifact in artifacts)
        entries.append((unsafe_info, data))
        path = target_dir / f"{name}.wosincident"
        path.write_bytes(raw_tar(entries))
        cases.append(
            case_record(
                name,
                path,
                root,
                workflow="load-reject",
                valid=False,
                degraded=None,
                issue_codes=[issue],
            )
        )

    payload = b"must-not-escape\n"
    add_raw("archive-path-traversal", tar_info("../escape", len(payload)), payload, "unsafe_member_path")
    add_raw(
        "archive-symlink",
        tar_info("artifacts/link", 0, kind=tarfile.SYMTYPE, linkname="../../escape"),
        None,
        "symlink_member_rejected",
    )
    add_raw(
        "archive-hardlink",
        tar_info("artifacts/hardlink", 0, kind=tarfile.LNKTYPE, linkname="manifest.json"),
        None,
        "hardlink_member_rejected",
    )
    device = tar_info("artifacts/device", 0, kind=tarfile.CHRTYPE)
    device.devmajor = 1
    device.devminor = 3
    add_raw("archive-device", device, None, "unsafe_member_type")

    truncated_manifest, truncated_artifacts = security_manifest("archive-truncated")
    complete_archive = archive_bytes(truncated_manifest, truncated_artifacts)
    truncated_path = target_dir / "archive-truncated.wosincident"
    # Stop part-way through manifest.json's payload, not merely in tar's
    # deterministic zero padding.  This remains malformed even when the
    # archive writer changes its final block factor.
    manifest_size = len(canonical_json(truncated_manifest))
    truncated_path.write_bytes(complete_archive[: 512 + max(1, manifest_size // 2)])
    cases.append(
        case_record(
            "archive-truncated",
            truncated_path,
            root,
            workflow="load-reject",
            valid=False,
            degraded=None,
            issue_codes=["archive_read_failed"],
        )
    )

    oversized_manifest, oversized_artifacts = security_manifest("archive-oversized-member")
    # USTAR's octal size field tops out just below 8 GiB.  Four GiB is still
    # comfortably above WOSDBG's default 128 MiB per-member policy while
    # remaining representable by every supported tar implementation.
    oversized_info = tar_info("artifacts/oversized.bin", 1 << 32)
    oversized_entries: list[tuple[tarfile.TarInfo, bytes | None]] = [
        (tar_info("manifest.json", len(canonical_json(oversized_manifest))), canonical_json(oversized_manifest))
    ]
    oversized_entries.extend(
        (tar_info(artifact.path, len(artifact.data)), artifact.data) for artifact in oversized_artifacts
    )
    # No body is intentional: a safe reader rejects the declared expansion
    # before attempting to allocate/read it, distinguishing size policy from
    # the later truncation that an unsafe reader would encounter.
    oversized_entries.append((oversized_info, None))
    oversized_path = target_dir / "archive-oversized-member.wosincident"
    oversized_path.write_bytes(raw_tar(oversized_entries))
    cases.append(
        case_record(
            "archive-oversized-member",
            oversized_path,
            root,
            workflow="load-reject",
            valid=False,
            degraded=None,
            issue_codes=["member_too_large"],
        )
    )
    return cases


def generate_collector_cases(root: Path) -> list[dict[str, Any]]:
    collector_root = root / "collector"
    cases: list[dict[str, Any]] = []
    elf = minimal_elf(BUILD_ID_A)

    ktest = collector_root / "ktest-partial"
    (ktest / "coredumps").mkdir(parents=True, exist_ok=True)
    (ktest / "configs").mkdir(parents=True, exist_ok=True)
    (ktest / "serial-vm0.log").write_bytes(log_bytes(0, timestamps="partial", final_newline=False))
    (ktest / "qemu-vm0.log").write_text("fixture qemu log\n", encoding="utf-8")
    (ktest / "coredumps" / "fixture-v3.bin").write_bytes(build_coredump(3, elf))
    (ktest / "configs" / "node_ktest.json").write_bytes(config_bytes([node(0, "wos-0")]))
    (ktest / "run-manifest.json").write_bytes(
        canonical_json({"kind": "coverage-run-manifest", "complete": False, "fixture": True})
    )
    cases.append(
        {
            "name": "ktest-partial",
            "path": ktest.relative_to(root).as_posix(),
            "sourceKind": "ktest",
            "expectedTruncated": ["serial-vm0.log"],
        }
    )

    cluster = collector_root / "cluster-partial"
    (cluster / "coredumps").mkdir(parents=True, exist_ok=True)
    (cluster / "configs").mkdir(parents=True, exist_ok=True)
    for index in range(2):
        (cluster / f"serial-vm{index}.log").write_bytes(log_bytes(index))
        (cluster / f"qemu-vm{index}.log").write_text(f"fixture qemu node {index}\n", encoding="utf-8")
        (cluster / "coredumps" / f"node{index}-v3.bin").write_bytes(
            build_coredump(3, elf, pid=42 + index, node_number=index)
        )
    (cluster / "configs" / "cluster.json").write_bytes(
        config_bytes([node(0, "wos-0"), node(1, "wos-1")])
    )
    cases.append(
        {
            "name": "cluster-partial",
            "path": cluster.relative_to(root).as_posix(),
            "sourceKind": "cluster",
            "expectedMissing": ["coverage-manifest"],
        }
    )
    return cases


def parse_coredump_preamble(data: bytes) -> tuple[int, int]:
    if len(data) < 16:
        raise ValueError("coredump fixture is shorter than its preamble")
    magic, version, header_size = struct.unpack_from("<QII", data)
    if magic != COREDUMP_MAGIC:
        raise ValueError("coredump fixture has bad magic")
    return version, header_size


def manifest_from_bundle(path: Path) -> tuple[dict[str, Any], dict[str, bytes]]:
    if path.is_dir():
        manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
        members = {
            member["path"]: path.joinpath(*PurePosixPath(member["path"]).parts).read_bytes()
            for member in manifest["members"]
        }
        return manifest, members
    with tarfile.open(path, mode="r:") as archive:
        manifest_file = archive.extractfile("manifest.json")
        if manifest_file is None:
            raise ValueError(f"{path} has no manifest.json")
        manifest = json.loads(manifest_file.read())
        members = {}
        for member in manifest["members"]:
            fileobj = archive.extractfile(member["path"])
            if fileobj is None:
                raise ValueError(f"{path} has no member {member['path']}")
            members[member["path"]] = fileobj.read()
        return manifest, members


def verify_generated_tree(root: Path) -> None:
    index_path = root / "cases.json"
    index = json.loads(index_path.read_text(encoding="utf-8"))
    if index.get("format") != "wosdbg-incident-fixtures" or index.get("version") != 1:
        raise AssertionError("fixture index format/version mismatch")
    names: set[str] = set()
    for group in ("loaderCases", "securityCases", "collectorCases"):
        for case in index[group]:
            if case["name"] in names:
                raise AssertionError(f"duplicate fixture case {case['name']}")
            names.add(case["name"])
            path = root / case["path"]
            if not path.exists():
                raise AssertionError(f"fixture path is missing: {path}")
            if case["name"].startswith("source-") and case["name"].endswith("-symlink") and not path.is_symlink():
                raise AssertionError(f"{case['name']}: top-level source fixture is not a symlink")

    for case in index["loaderCases"]:
        bundle = root / case["path"]
        manifest, member_data = manifest_from_bundle(bundle)
        if manifest["format"] != "wosincident" or manifest["version"] != 1:
            raise AssertionError(f"{case['name']}: bad manifest format/version")
        incident_id = manifest["incidentId"]
        if not isinstance(incident_id, str) or not incident_id.startswith("sha256:") or len(incident_id) != 71:
            raise AssertionError(f"{case['name']}: incidentId is not a content-derived SHA-256 identity")
        identity = dict(manifest)
        identity.pop("incidentId")
        identity.pop("createdUtc")
        identity_bytes = json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        if incident_id != "sha256:" + sha256(identity_bytes):
            raise AssertionError(f"{case['name']}: incidentId does not match manifest identity content")
        checksum_mismatches = 0
        for member in manifest["members"]:
            validate_member_path(member["path"])
            data = member_data[member["path"]]
            if len(data) != member["size"]:
                raise AssertionError(f"{case['name']}: member size mismatch for {member['path']}")
            if sha256(data) != member["sha256"]:
                checksum_mismatches += 1
            if PurePosixPath(member["sourceName"]).name != member["sourceName"]:
                raise AssertionError(f"{case['name']}: sourceName is not a basename")
            if member["kind"] == "coredump" and not member["truncated"] and case["name"] != "corrupt-coredump":
                version, header_size = parse_coredump_preamble(data)
                if version in {1, 2, 3} and header_size not in {488, 1360, 1840}:
                    raise AssertionError(f"{case['name']}: unexpected coredump header size")
        expected_mismatches = 1 if case["name"] == "checksum-failure" else 0
        if checksum_mismatches != expected_mismatches:
            raise AssertionError(
                f"{case['name']}: found {checksum_mismatches} checksum mismatches, expected {expected_mismatches}"
            )


def tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            kind = b"L"
            data = str(path.readlink()).encode("utf-8")
        elif path.is_file():
            kind = b"F"
            data = path.read_bytes()
        else:
            continue
        relative = path.relative_to(root).as_posix().encode("utf-8")
        digest.update(struct.pack("<Q", len(relative)))
        digest.update(relative)
        digest.update(kind)
        digest.update(struct.pack("<Q", len(data)))
        digest.update(data)
    return digest.hexdigest()


def generate(root: Path) -> dict[str, Any]:
    if root.exists() and any(root.iterdir()):
        raise ValueError(f"fixture output directory is not empty: {root}")
    root.mkdir(parents=True, exist_ok=True)
    index = {
        "format": "wosdbg-incident-fixtures",
        "version": 1,
        "createdUtc": FIXTURE_CREATED_UTC,
        "loaderCases": generate_loader_cases(root),
        "securityCases": generate_security_cases(root),
        "collectorCases": generate_collector_cases(root),
    }
    (root / "cases.json").write_bytes(canonical_json(index))
    verify_generated_tree(root)
    return index


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="Empty or disposable fixture output directory")
    parser.add_argument("--print-index", action="store_true", help="Print the generated case index as canonical JSON")
    args = parser.parse_args()

    index = generate(args.output.resolve())
    print(f"generated {len(index['loaderCases'])} loader, {len(index['securityCases'])} security, "
          f"and {len(index['collectorCases'])} collector fixtures; digest={tree_digest(args.output.resolve())}")
    if args.print_index:
        print(canonical_json(index).decode("utf-8"), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
