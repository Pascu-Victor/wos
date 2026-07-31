#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
VALIDATOR_PATH = ROOT / "scripts" / "bench" / "validate_wos_memory_balance.py"
REGRESSION_PATH = ROOT / "scripts" / "bench" / "run_wos_memory_balance_regression.py"


def load_validator():
    spec = importlib.util.spec_from_file_location("validate_wos_memory_balance", VALIDATOR_PATH)
    if spec is None or spec.loader is None:
        raise AssertionError("failed to load memory-balance validator")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_regression():
    sys.path.insert(0, str(REGRESSION_PATH.parent))
    try:
        spec = importlib.util.spec_from_file_location("run_wos_memory_balance_regression", REGRESSION_PATH)
        if spec is None or spec.loader is None:
            raise AssertionError("failed to load memory-balance regression runner")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.pop(0)


def summary_text() -> str:
    page_size = 4096
    summary = (
        "summary schema=2 total_pages=100 total_bytes=409600 "
        "free_pages=80 free_bytes=327680 used_pages=20 used_bytes=81920 "
        "owner_pages=10 owner_bytes=40960 allocator_metadata_pages=5 allocator_metadata_bytes=20480 "
        "zone_descriptor_pages=2 zone_descriptor_bytes=8192 "
        "per_cpu_page_cache_reserve_pages=3 per_cpu_page_cache_reserve_bytes=12288 "
        "identity_pages=100 identity_bytes=409600 firmware_reserve_pages=0 firmware_reserve_bytes=0 "
        "physical_address_pages=100 physical_address_identity_pages=100 "
        "identity_mismatch_pages=0 untracked_unreclaimable_pages=0\n"
    )

    def owner(name: str, pages: int, objects: int, reclaimability: str) -> str:
        return (
            f"physical_owner name={name} pages={pages} bytes={pages * page_size} objects={objects} "
            f"lifetime=test_lifetime reclaimability={reclaimability} scaling_bound=test_bound\n"
        )

    return (
        "== summary ==\n"
        + summary
        + owner("user_private_mapping", 10, 10, "owner_teardown")
        + owner("physical_allocator_embedded_metadata", 5, 1, "permanent_reserve")
        + owner("physical_zone_descriptors", 2, 2, "permanent_reserve")
        + owner("per_cpu_physical_page_cache_metadata", 3, 3, "permanent_reserve")
        + "== procs ==\n"
        + "proc pid=1 name=init cmd=/init\n"
        + "== features ==\n"
        + "feature name=page_callers available=0 default=0 enabled=0 generation=0\n"
        + "feature name=kmalloc_debug available=0 default=0 enabled=0 generation=0\n"
    )


def netdiag_text() -> str:
    return (
        "wki_ipc exports=0 proxies=0 active_pumps=0 ring_used=0 pending_deliveries=0 pending_bytes=0 "
        "export_flush_queue=0 export_close_pending=0 export_close_waiting_for_bytes=0 proxy_close_queue=0 dev_op_queue=0\n"
        "wki_compute submitted_active=0 running_active=0 pending_complete=0 truncated=0\n"
        "wki_vfs_server active=0 retiring=0\n"
        "wki_vfs_proxy active=1 op_pending=0 attach_pending=0 mount=/wki/wos-1\n"
        "wki_channel peer=1 state=CONNECTED truncated_final_field\n"
    )


def create_checkpoint(root: Path, *, memacc: str | None = None, netdiag: str | None = None) -> Path:
    checkpoint = root / "clean-boot"
    checkpoint.mkdir()
    host = "wos-0"
    (checkpoint / f"{host}-memacc-all.txt").write_text(memacc if memacc is not None else summary_text())
    (checkpoint / f"{host}-memacc-summary.txt").write_text(summary_text().split("== procs ==", 1)[0])
    (checkpoint / f"{host}-wki-netdiag.txt").write_text(netdiag if netdiag is not None else netdiag_text())
    (checkpoint / f"{host}-kipcstat.txt").write_text("exports=0 proxies=0\n")
    (checkpoint / f"{host}-kcpustate.txt").write_text("loadavg_state active=0\n")
    (checkpoint / f"{host}-meminfo.txt").write_text("MemTotal: 400 KiB\n")
    (checkpoint / "status.tsv").write_text(
        "checkpoint\ttimestamp_utc\thost\ttotal_pages\tfree_pages\towner_pages\tidentity_pages\t"
        "identity_mismatch_pages\tuntracked_unreclaimable_pages\tstatus\n"
        "clean-boot\t2026-07-31T00:00:00Z\twos-0\t100\t80\t10\t100\t0\t0\tpass\n"
    )
    return checkpoint


def expect_rejected(module, checkpoint: Path, expected_fragment: str, *, quiescent: bool = False) -> None:
    try:
        module.validate_checkpoint(checkpoint, "clean-boot", ["wos-0"], quiescent)
    except module.ValidationError as exc:
        if expected_fragment not in str(exc):
            raise AssertionError(f"expected {expected_fragment!r} in validation error: {exc}") from exc
        return
    raise AssertionError(f"validator accepted injected fault: {expected_fragment}")


def test_valid_checkpoint(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-memory-balance-valid-") as temp:
        checkpoint = create_checkpoint(Path(temp))
        payload = module.validate_checkpoint(checkpoint, "clean-boot", ["wos-0"], True)
        if payload["status"] != "pass" or payload["nodes"]["wos-0"]["identity_mismatch_pages"] != 0:
            raise AssertionError("valid checkpoint did not produce an exact pass payload")
        if len(payload["sha256"]["wos-0"]) != 6:
            raise AssertionError("checkpoint payload does not cover all raw evidence files")


def test_identity_faults_are_rejected(module) -> None:
    mutations = (
        ("identity_mismatch_pages=0", "identity_mismatch_pages=1", "identity_mismatch_pages=1"),
        ("untracked_unreclaimable_pages=0", "untracked_unreclaimable_pages=1", "untracked_unreclaimable_pages=1"),
        ("identity_pages=100", "identity_pages=99", "computed_identity=100"),
        ("owner_pages=10", "owner_pages=11", "computed_identity=101"),
    )
    for old, new, expected in mutations:
        with tempfile.TemporaryDirectory(prefix="wos-memory-balance-identity-fault-") as temp:
            checkpoint = create_checkpoint(Path(temp), memacc=summary_text().replace(old, new, 1))
            expect_rejected(module, checkpoint, expected)


def test_descriptor_faults_are_rejected(module) -> None:
    mutations = (
        ("name=user_private_mapping", "name=unaccounted_estimate", "forbidden residual category"),
        (" scaling_bound=test_bound", "", "incomplete descriptor fields"),
        ("reclaimability=owner_teardown", "reclaimability=reclaimable_maybe", "unsupported reclaimability"),
    )
    for old, new, expected in mutations:
        with tempfile.TemporaryDirectory(prefix="wos-memory-balance-descriptor-fault-") as temp:
            checkpoint = create_checkpoint(Path(temp), memacc=summary_text().replace(old, new, 1))
            expect_rejected(module, checkpoint, expected)


def test_quiescence_faults_are_rejected(module) -> None:
    mutations = (
        ("running_active=0", "running_active=1", "wki_compute.running_active=1"),
        ("op_pending=0", "op_pending=1", "wki_vfs_proxy.op_pending=1"),
        ("wki_vfs_server active=0", "wki_compute_task pid=42\nwki_vfs_server active=0", "retained WKI"),
    )
    for old, new, expected in mutations:
        with tempfile.TemporaryDirectory(prefix="wos-memory-balance-quiescence-fault-") as temp:
            checkpoint = create_checkpoint(Path(temp), netdiag=netdiag_text().replace(old, new, 1))
            expect_rejected(module, checkpoint, expected, quiescent=True)

    with tempfile.TemporaryDirectory(prefix="wos-memory-balance-build-fault-") as temp:
        memacc = summary_text().replace("name=init cmd=/init", "name=ninja cmd=/usr/bin/ninja", 1)
        checkpoint = create_checkpoint(Path(temp), memacc=memacc)
        expect_rejected(module, checkpoint, "retained build process", quiescent=True)

    with tempfile.TemporaryDirectory(prefix="wos-memory-provenance-fault-") as temp:
        memacc = summary_text().replace(
            "name=page_callers available=0 default=0 enabled=0",
            "name=page_callers available=1 default=0 enabled=1",
            1,
        )
        checkpoint = create_checkpoint(Path(temp), memacc=memacc)
        expect_rejected(module, checkpoint, "diagnostic feature page_callers.enabled=1")


def reclaim_memacc(*, buffer_bytes: int, packet_capacity: int, packet_baseline: int, idle_inodes: int, file_pages: int) -> str:
    return (
        "== reclaim/buffer_cache ==\n"
        f"reclaim name=buffer_cache total_bytes={buffer_bytes}\n"
        "== reclaim/packet_pool ==\n"
        f"reclaim name=packet_pool capacity={packet_capacity} baseline_capacity={packet_baseline}\n"
        "== reclaim/xfs_inode ==\n"
        f"reclaim name=xfs_inode idle_inodes={idle_inodes}\n"
        "== reclaim/file_mmap_cache ==\n"
        f"reclaim name=file_mmap_cache pages={file_pages}\n"
    )


def reclaim_snapshot(pages: int) -> dict:
    owner_names = (
        "user_file_cache",
        "buffer_cache_data",
        "buffer_cache_metadata",
        "xfs_inode_metadata",
        "network_packet",
    )
    return {
        "nodes": {
            "wos-0": {
                "owners": [
                    {"name": name, "pages": pages, "reclaimability": "pressure_reclaim"}
                    for name in owner_names
                ]
            }
        }
    }


def test_regression_runner_fault_gates(_module) -> None:
    regression = load_regression()
    config = regression.validate_config(ROOT / "configs" / "cluster_selfhost_4_host32.json", ["wos-0", "wos-1", "wos-2", "wos-3"])
    if not config["zones"]:
        raise AssertionError("host32 regression config validation produced no zones")

    overhead = regression.overhead_result([1000, 1000, 1000], [1019, 1020, 1021])
    if overhead["status"] != "pass":
        raise AssertionError("2% accounting overhead boundary should pass")
    try:
        regression.overhead_result([1000, 1000, 1000], [1021, 1021, 1021])
    except regression.RegressionError:
        pass
    else:
        raise AssertionError("injected >2% accounting overhead was accepted")

    with tempfile.TemporaryDirectory(prefix="wos-memory-boot-id-gate-") as temp:
        root = Path(temp)
        serial_logs = [root / "serial-vm0.log", root / "serial-vm1.log"]
        serial_logs[0].write_text("WOS version=test boot_id=aaaa\n")
        serial_logs[1].write_text("WOS version=test boot_id=bbbb\n")
        boot_ids = regression.read_serial_boot_ids(serial_logs, ["wos-0", "wos-1"])
        if boot_ids != {"wos-0": "aaaa", "wos-1": "bbbb"}:
            raise AssertionError("serial boot-ID vector was not parsed exactly")
        serial_logs[1].write_text("WOS version=test boot_id=cccc\n")
        try:
            regression.require_unchanged_boot_ids(serial_logs, ["wos-0", "wos-1"], boot_ids, "injected reboot")
        except regression.RegressionError as exc:
            if "cluster boot IDs changed" not in str(exc):
                raise
        else:
            raise AssertionError("injected cross-checkpoint reboot was accepted")

    with tempfile.TemporaryDirectory(prefix="wos-memory-reclaim-gate-") as temp:
        root = Path(temp)
        before_dir = root / "before"
        after_dir = root / "after"
        before_dir.mkdir()
        after_dir.mkdir()
        (before_dir / "wos-0-memacc-all.txt").write_text(
            reclaim_memacc(buffer_bytes=100, packet_capacity=20, packet_baseline=10, idle_inodes=5, file_pages=7)
        )
        (after_dir / "wos-0-memacc-all.txt").write_text(
            reclaim_memacc(buffer_bytes=20, packet_capacity=10, packet_baseline=10, idle_inodes=0, file_pages=0)
        )
        before = reclaim_snapshot(10)
        after = reclaim_snapshot(5)
        result = regression.validate_reclaim(before, after, before_dir, after_dir, ["wos-0"])
        if result["status"] != "pass":
            raise AssertionError("valid reclaim transition did not pass")

        fault = reclaim_snapshot(5)
        for owner in fault["nodes"]["wos-0"]["owners"]:
            if owner["name"] == "network_packet":
                owner["pages"] = 10
        try:
            regression.validate_reclaim(before, fault, before_dir, after_dir, ["wos-0"])
        except regression.RegressionError as exc:
            if "network_packet did not release" not in str(exc):
                raise
        else:
            raise AssertionError("injected reclaim-owner fault was accepted")

        (after_dir / "wos-0-memacc-all.txt").write_text(
            reclaim_memacc(buffer_bytes=100, packet_capacity=10, packet_baseline=10, idle_inodes=0, file_pages=0)
        )
        try:
            regression.validate_reclaim(before, after, before_dir, after_dir, ["wos-0"])
        except regression.RegressionError as exc:
            if "buffer_cache reclaim endpoint released nothing" not in str(exc):
                raise
        else:
            raise AssertionError("injected reclaim-endpoint fault was accepted")


def main() -> None:
    module = load_validator()
    tests = (
        test_valid_checkpoint,
        test_identity_faults_are_rejected,
        test_descriptor_faults_are_rejected,
        test_quiescence_faults_are_rejected,
        test_regression_runner_fault_gates,
    )
    for test in tests:
        test(module)
    print(f"{len(tests)} WOS memory-balance validator tests passed")


if __name__ == "__main__":
    main()
