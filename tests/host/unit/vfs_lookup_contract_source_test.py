#!/usr/bin/env python3
"""Check the pathname API inventory and remote-owner scalar admission contract."""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DOC = ROOT / "docs/vfs_lookup_contract.md"
VFS_HEADER = ROOT / "modules/kern/src/vfs/vfs.hpp"
SYSCALL_SOURCE = ROOT / "modules/kern/src/syscalls_impl/vfs/sys_vfs.cpp"
REMOTE_SOURCE = ROOT / "modules/kern/src/net/wki/remote_vfs.cpp"
DEV_SERVER_SOURCE = ROOT / "modules/kern/src/net/wki/dev_server.cpp"
MOUNT_SOURCE = ROOT / "modules/kern/src/vfs/mount.cpp"
CORE_SOURCE = ROOT / "modules/kern/src/vfs/core.cpp"

FAMILIES = {
    "path.open": {"vfs_open_directory_view_as", "vfs_open_file_as", "vfs_open", "vfs_openat", "vfs_open_file", "vfs_open_file_resolved", "vfs_open_file_resolved_beneath", "vfs_open_read_file_as", "vfs_open_read_file_resolved_beneath_as", "vfs_open_file_resolved_beneath_as"},
    "path.stat": {"vfs_stat", "vfs_lstat", "vfs_stat_resolved", "vfs_stat_resolved_beneath", "vfs_stat_resolved_beneath_as", "vfs_statat", "vfs_statvfs"},
    "path.access": {"vfs_access", "vfs_faccessat"},
    "path.readlink": {"vfs_readlink", "vfs_readlinkat", "vfs_readlink_resolved", "vfs_readlink_resolved_beneath", "vfs_readlink_resolved_beneath_as", "vfs_realpath", "vfs_realpath_resolved_beneath_as"},
    "path.mkdir": {"vfs_mkdir", "vfs_mkdirat", "vfs_mkdir_resolved_beneath", "vfs_mkdir_resolved_beneath_as"},
    "path.symlink": {"vfs_symlink", "vfs_symlink_resolved", "vfs_symlink_resolved_beneath", "vfs_symlink_resolved_beneath_as", "vfs_symlinkat"},
    "path.remove": {"vfs_unlink", "vfs_unlink_resolved", "vfs_unlink_resolved_beneath", "vfs_unlink_resolved_beneath_as", "vfs_rmdir", "vfs_unlinkat"},
    "path.rename": {"vfs_rename", "vfs_rename_resolved", "vfs_rename_resolved_beneath", "vfs_rename_resolved_beneath_as", "vfs_renameat"},
    "path.link": {"vfs_link", "vfs_linkat"},
    "path.mode": {"vfs_chmod", "vfs_chmod_resolved", "vfs_chmod_resolved_beneath", "vfs_chmod_resolved_beneath_as", "vfs_fchmodat"},
    "path.owner": {"vfs_chown", "vfs_fchownat", "vfs_chown_resolved_beneath_as"},
    "path.time": {"vfs_utimens_resolved_beneath", "vfs_utimens_resolved_beneath_as", "vfs_utimensat"},
    "path.xattr": {
        "vfs_xattr_resolved_beneath_as",
        "vfs_setxattr",
        "vfs_setxattr_beneath",
        "vfs_getxattr",
        "vfs_getxattr_beneath",
        "vfs_listxattr",
        "vfs_listxattr_beneath",
        "vfs_removexattr",
        "vfs_removexattr_beneath",
    },
    "path.chdir": {"vfs_chdir", "vfs_pin_directory_resolved_beneath_as", "vfs_pin_directory_view_as"},
    "path.mount": {"vfs_mount", "vfs_mount_for_task", "vfs_umount", "vfs_umount_for_task", "vfs_pivot_root", "vfs_pivot_root_for_task", "vfs_shutdown_unmount_all"},
    "path.batch": {"vfs_metadata_batch"},
    "path.resolve-helper": {"vfs_resolve_dirfd", "vfs_directory_path_beneath"},
    "path.route-policy": {
        "vfs_wki_rule_add",
        "vfs_wki_rule_get",
        "vfs_wki_default_rule_get",
        "vfs_wki_effective_route_for_path",
    },
    "path.cache-observer": {"vfs_cache_notify_invalidate_path", "vfs_cache_notify_path_changed"},
    "path.selftest": {"vfs_selftest_remote_readonly_resolved"},
}

PATH_SYSCALLS = {
    "OPEN",
    "OPENAT",
    "STAT",
    "LSTAT",
    "STATAT",
    "STATVFS",
    "ACCESS",
    "FACCESSAT",
    "READLINK",
    "READLINKAT",
    "REALPATH",
    "MKDIR",
    "MKDIRAT",
    "SYMLINK",
    "SYMLINKAT",
    "UNLINK",
    "RMDIR",
    "UNLINKAT",
    "RENAME",
    "RENAMEAT",
    "LINK",
    "LINKAT",
    "CHMOD",
    "FCHMODAT",
    "CHOWN",
    "FCHOWNAT",
    "UTIMENSAT",
    "SETXATTR",
    "LSETXATTR",
    "GETXATTR",
    "LGETXATTR",
    "LISTXATTR",
    "LLISTXATTR",
    "REMOVEXATTR",
    "LREMOVEXATTR",
    "CHDIR",
    "MOUNT",
    "UMOUNT",
    "PIVOT_ROOT",
    "METADATA_BATCH",
    "WKI_RULE_ADD",
}

PATH_PARAMETER = re.compile(
    r"\b(?:path|pathname|oldpath|newpath|linkpath|new_root|put_old|export_root|relative_path|"
    r"confinement_root|root_path|vfs_path|old_vfs_path|new_vfs_path|prefix|target)\b"
)


def fail(message: str) -> None:
    print(f"vfs lookup contract source test failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def inventory_section(document: str) -> str:
    begin = "<!-- VFS_PATH_INVENTORY_BEGIN -->"
    end = "<!-- VFS_PATH_INVENTORY_END -->"
    if document.count(begin) != 1 or document.count(end) != 1:
        fail("inventory markers must each occur exactly once")
    section = document.split(begin, 1)[1].split(end, 1)[0]
    if not section.strip():
        fail("inventory is empty")
    return section


def public_path_apis(header: str) -> set[str]:
    declarations = re.findall(r"(?:auto|void)\s+(vfs_[A-Za-z0-9_]+)\s*\((.*?)\)\s*(?:->\s*[^;]+)?;", header, re.DOTALL)
    discovered = {name for name, parameters in declarations if PATH_PARAMETER.search(parameters)}
    # MetadataBatchEntry carries its paths rather than spelling them in the API parameters.
    discovered.add("vfs_metadata_batch")
    return discovered


def check_inventory(document: str, header: str, syscall_source: str) -> None:
    section = inventory_section(document)
    family_rows = [line for line in section.splitlines() if line.startswith("| `path.")]
    inventory_apis: set[str] = set()
    for row in family_rows:
        columns = row.split("|")
        if len(columns) < 6:
            fail(f"malformed inventory row: {row}")
        inventory_apis.update(re.findall(r"`(vfs_[A-Za-z0-9_]+)`", columns[2]))
    expected_apis = set().union(*FAMILIES.values())
    discovered_apis = public_path_apis(header)
    declared_apis = set(re.findall(r"(?:auto|void)\s+(vfs_[A-Za-z0-9_]+)\s*\(", header))

    missing_rows = sorted(family for family in FAMILIES if f"`{family}`" not in section)
    if missing_rows:
        fail(f"missing family rows: {', '.join(missing_rows)}")
    missing_expected = sorted(expected_apis - inventory_apis)
    if missing_expected:
        fail(f"required pathname APIs absent from inventory: {', '.join(missing_expected)}")
    undeclared_expected = sorted(expected_apis - declared_apis)
    if undeclared_expected:
        fail(f"inventoried pathname APIs have no public declaration: {', '.join(undeclared_expected)}")
    missing_discovered = sorted(discovered_apis - inventory_apis)
    if missing_discovered:
        fail(f"public pathname APIs absent from inventory: {', '.join(missing_discovered)}")
    stale_inventory = sorted(inventory_apis - expected_apis)
    if stale_inventory:
        fail(f"inventory names are not public pathname declarations: {', '.join(stale_inventory)}")

    public_at_entrypoints = {name for name in discovered_apis if name.endswith("at")}
    missing_at = sorted(public_at_entrypoints - inventory_apis)
    if missing_at:
        fail(f"public *at entrypoints absent from inventory: {', '.join(missing_at)}")

    documented_ops = set(re.findall(r"`ops::([A-Z0-9_]+)`", section))
    missing_ops = sorted(PATH_SYSCALLS - documented_ops)
    if missing_ops:
        fail(f"pathname syscall operations absent from inventory: {', '.join(missing_ops)}")
    absent_cases = sorted(op for op in PATH_SYSCALLS if f"case ops::{op}:" not in syscall_source)
    if absent_cases:
        fail(f"inventoried syscall operations have no dispatcher case: {', '.join(absent_cases)}")


def opcode_block(handler: str, opcode: str) -> str:
    # A nested selector switch may have its own default; only peer opcode
    # labels at the same indentation terminate this outer dispatch block.
    match = re.search(
        rf"^(?P<indent>[ \t]*)case {re.escape(opcode)}:\s*\{{(.*?)(?=^(?P=indent)(?:case OP_VFS_|default:))",
        handler, re.DOTALL | re.MULTILINE,
    )
    if match is None:
        fail(f"remote owner handler is missing {opcode}")
    return match.group(2)


def check_remote_owner_admission(source: str) -> None:
    handler_start = source.find("void handle_vfs_op(")
    handler_end = source.find("void handle_vfs_op_resp(", handler_start)
    if handler_start < 0 or handler_end < 0:
        fail("cannot locate remote owner pathname dispatcher")
    handler = source[handler_start:handler_end]

    helpers = {
        "relative_wire_path_has_safe_components": r"auto\s+relative_wire_path_has_safe_components\s*\(",
        "build_full_path": r"auto\s+build_full_path\s*\([^)]*\)\s*->\s*int",
        "exact_relative_wire_path": r"auto\s+exact_relative_wire_path\s*\(",
        "wire_payload_has_optional_exact_suffix": r"auto\s+wire_payload_has_optional_exact_suffix\s*\(",
        "wire_symlink_target_is_representable": r"auto\s+wire_symlink_target_is_representable\s*\(",
        "prepare_legacy_scalar_path": r"auto\s+prepare_legacy_scalar_path\s*\(",
    }
    for name, pattern in helpers.items():
        if re.search(pattern, source, re.DOTALL) is None:
            fail(f"remote owner admission helper disappeared: {name}")
    if "return -ENAMETOOLONG;" not in source:
        fail("checked full-path construction no longer exposes -ENAMETOOLONG")

    legacy_chmod = handler.split("case OP_VFS_CHMOD:", 1)[1].split("case OP_VFS_UTIMENS:", 1)[0]
    if "reject_request(-ENOTSUP)" not in legacy_chmod or "vfs_chmod_resolved_beneath(" in legacy_chmod:
        fail("legacy chmod must reject callerless authority")
    mapped_paths = opcode_block(handler, "OP_VFS_PATH_AS")
    for token in ("wki_vfs_chmod_request_valid", "map_vfs_mutation_identity", "vfs_chmod_resolved_beneath_as"):
        if token not in mapped_paths: fail("mapped chmod lost explicit framing, identity or confined owner dispatch")

    legacy_mkdir = handler.split("case OP_VFS_MKDIR:", 1)[1].split("case OP_VFS_READLINK:", 1)[0]
    if "reject_request(-ENOTSUP)" not in legacy_mkdir or "vfs_mkdir_resolved_beneath(" in legacy_mkdir:
        fail("legacy mkdir must reject callerless authority")
    for token in ("wki_vfs_mkdir_request_valid", "map_vfs_mutation_identity", "vfs_mkdir_resolved_beneath_as"):
        if token not in mapped_paths: fail("mapped mkdir lost framing, identity or confined owner dispatch")

    legacy_remove = handler.split("case OP_VFS_UNLINK:", 1)[1].split("case OP_VFS_RENAME:", 1)[0]
    if "reject_request(-ENOTSUP)" not in legacy_remove or "vfs_unlink_resolved_beneath(" in legacy_remove:
        fail("legacy unlink/rmdir must reject callerless authority")
    for token in ("wki_vfs_remove_request_valid", "map_vfs_mutation_identity", "vfs_unlink_resolved_beneath_as"):
        if token not in mapped_paths: fail("mapped removal lost framing, identity or confined owner dispatch")

    legacy_rename = handler.split("case OP_VFS_RENAME:", 1)[1].split("case OP_VFS_CHMOD:", 1)[0]
    if "reject_request(-ENOTSUP)" not in legacy_rename or "vfs_rename_resolved_beneath(" in legacy_rename:
        fail("legacy rename must refuse callerless identity")
    for token in ("wki_vfs_rename_request_valid", "map_vfs_mutation_identity", "wki_dev_server_retain_vfs_sibling", "vfs_rename_resolved_beneath_as"):
        if token not in mapped_paths: fail("mapped rename lost identity or exact retained-directory dispatch")

    legacy_symlink = handler.split("case OP_VFS_SYMLINK:", 1)[1].split("case OP_VFS_UNLINK:", 1)[0]
    if "reject_request(-ENOTSUP)" not in legacy_symlink or "vfs_symlink_resolved_beneath(" in legacy_symlink:
        fail("legacy symlink must reject callerless authority")
    for token in ("wki_vfs_symlink_request_valid", "map_vfs_mutation_identity", "vfs_symlink_resolved_beneath_as"):
        if token not in mapped_paths: fail("mapped symlink lost framing, identity or confined owner dispatch")

    scalar_ops = {
        "OP_VFS_OPEN_AS",
        "OP_VFS_STAT_AS",
        "OP_VFS_PATH_AS",
    }
    for opcode in scalar_ops:
        block = opcode_block(handler, opcode)
        mapped = opcode in {"OP_VFS_OPEN_AS", "OP_VFS_STAT_AS", "OP_VFS_PATH_AS"}
        expected = "prepare_mapped_scalar_path" if mapped else "prepare_legacy_scalar_path"
        if expected not in block:
            fail(f"{opcode} no longer performs checked export-path admission")
        if "reject_request" not in block:
            fail(f"{opcode} no longer rejects malformed scalar input before VFS dispatch")

    for opcode in {"OP_VFS_STAT_AS", "OP_VFS_PATH_AS"}:
        if "exact_relative_wire_path" not in opcode_block(handler, opcode):
            fail(f"{opcode} no longer rejects trailing scalar payload")
    if "wire_payload_has_optional_exact_suffix" not in opcode_block(handler, "OP_VFS_OPEN_AS"):
        fail("OP_VFS_OPEN_AS no longer enforces its exact optional-suffix framing")
    if "wire_symlink_target_is_representable" not in opcode_block(handler, "OP_VFS_PATH_AS"):
        fail("OP_VFS_SYMLINK no longer bounds/rejects its target bytes")
    if "reject_request(-ENOTSUP)" not in handler.split("case OP_VFS_UTIMENS:", 1)[1].split("case OP_VFS_FSYNC:", 1)[0]:
        fail("legacy callerless UTIMENS must be refused")
    mapped_times = opcode_block(handler, "OP_VFS_PATH_AS")
    if "wki_vfs_utimens_request_valid" not in mapped_times or "vfs_utimens_resolved_beneath_as" not in mapped_times:
        fail("mapped UTIMENS must validate framing and use admitted owner lookup")


def check_remote_batch_uses_retained_owner_adapters(source: str) -> None:
    start = source.find("void handle_vfs_metadata_batch_op(")
    end = source.find("void send_cached_vfs_write_response(", start)
    if start < 0 or end < 0:
        fail("cannot locate remote metadata-batch owner handler")
    handler = source[start:end]
    required = {
        "vfs_open_file_resolved_beneath",
        "vfs_stat_resolved_beneath",
        "vfs_unlink_resolved_beneath",
        "vfs_rename_resolved_beneath",
    }
    missing = sorted(token for token in required if token not in handler)
    if missing:
        fail(f"metadata batch bypasses retained owner adapters: {', '.join(missing)}")


def check_mount_publication_is_two_phase(mount_source: str, core_source: str, remote_source: str) -> None:
    for token in ("prepare_mount_filesystem", "publish_prepared_mount", "finish_prepared_mount"):
        if token not in mount_source:
            fail(f"two-phase mount primitive disappeared: {token}")

    start = core_source.find("auto vfs_mount(")
    end = core_source.find("\nvoid init()", start)
    if start < 0 or end < 0:
        fail("cannot locate vfs_mount implementation")
    mount_handler = core_source[start:end].split("#ifdef WOS_SELFTEST", 1)[0]
    for token in ("LookupIntent::MOUNT_TARGET", "prepare_mount_filesystem", "consume_mount_target", "finish_prepared_mount"):
        if token not in mount_handler:
            fail(f"vfs_mount no longer retains and publishes through the two-phase contract: {token}")
    consume_start = core_source.index("auto consume_mount_target(")
    consume = core_source[consume_start:start]
    for token in ("validate_mount_target_lookup(target, nullptr)", "publish_prepared_mount", "&task, &credentials"):
        if token not in consume:
            fail(f"mount target consumption lost explicit admission: {token}")
    if ("vfs_consume_lookup(" in mount_handler or mount_handler.count("consume_mount_target(") != 2
            or mount_handler.count("consume_prepared_mount_target(") != 1):
        fail("every public mount stage must use its explicit admitted task and snapshot")
    if "RemoteVfsMountPublisher" not in remote_source or "publisher != nullptr" not in remote_source:
        fail("remote mount no longer delegates its publication to retained-target validation")


def check_direct_input_admission(core: str) -> None:
    start = core.index("auto vfs_open_file_as(")
    body = core[start:core.index("auto vfs_open_file_resolved(", start)]
    for token in ("REQUEST_CREDENTIALS = credentials", "&REQUEST_CREDENTIALS", "open_remote_read_file_as",
                  "open_procfs_lookup_as", "consume_lookup_locked", "*output = context.file"):
        if token not in body:
            fail(f"direct input must preserve explicit retained admission: {token}")
    for forbidden in ("task->credentials()", "FdReservation", "open_local_serialized", "vfs_open_file_impl"):
        if forbidden in body:
            fail(f"direct input must not replace its retained admission: {forbidden}")
    if "flags & ~(O_LOCAL | O_NOTIFY_CACHE_CHANGE | O_NO_CACHE)" not in body:
        fail("direct input must reject mutation and unsupported flags")
    start = core.index("auto open_procfs_lookup_as(", core.index("auto open_procfs_lookup_as(") + 1)
    proc = core[start:core.index("auto metadata_procfs_lookup_as(", start)]
    if "direct_output == nullptr" not in proc or "procfs_revalidate_open_result" not in proc:
        fail("direct proc input must retain target revalidation without descriptor reservation")


def check_remote_cwd_lifetime(core: str) -> None:
    change = core[core.index("auto change_directory_at("):core.index("auto vfs_chdir_common_local_fast_path(")]
    assert "auto const CREDENTIALS = task->credentials();" in change
    assert "apply_task_route, &lookup, 0, false, &CREDENTIALS)" in change
    assert "path_owner->cwd_anchor.swap(args.candidate)" in change
    assert change.index("Context context{.task = task}") < change.index("MutexGuard publication_guard")
    prepare = core[core.index("auto prepare_remote_cwd("):core.index("auto change_directory_at(")]
    assert prepare.index("MountOpenFileReservation reservation") < prepare.index("wki_remote_vfs_pin_directory(")
    assert "lookup.remote_relative_path() : strip_mount_prefix" in prepare
    assert "RELATIVE ? lookup.dirfd_anchor() : nullptr" in prepare
    assert "path_prefix_matches(mount_path.data(), root.data()" in prepare
    cwd = core[core.index("auto vfs_getcwd_for_task("):core.index("auto vfs_getcwd(char*")]
    assert cwd.index("remote_credentials == nullptr") < cwd.index("anchor = owner->cwd_anchor")
    assert "anchor.get() == owner->cwd_anchor.get()" in cwd
    public = core[core.index("auto vfs_getcwd(char*"):core.index("auto prepare_remote_cwd(")]
    assert "MutexGuard" not in public
    assert "&CREDENTIALS" in public


def main() -> None:
    for path in (DOC, VFS_HEADER, SYSCALL_SOURCE, REMOTE_SOURCE, DEV_SERVER_SOURCE, MOUNT_SOURCE, CORE_SOURCE):
        if not path.is_file():
            fail(f"missing required source: {path.relative_to(ROOT)}")
    check_remote_cwd_lifetime(CORE_SOURCE.read_text())
    check_direct_input_admission(CORE_SOURCE.read_text())
    check_inventory(DOC.read_text(), VFS_HEADER.read_text(), SYSCALL_SOURCE.read_text())
    check_remote_owner_admission(REMOTE_SOURCE.read_text())
    check_remote_batch_uses_retained_owner_adapters(DEV_SERVER_SOURCE.read_text())
    check_mount_publication_is_two_phase(MOUNT_SOURCE.read_text(), CORE_SOURCE.read_text(), REMOTE_SOURCE.read_text())
    print("vfs lookup contract source checks passed")


if __name__ == "__main__":
    main()
