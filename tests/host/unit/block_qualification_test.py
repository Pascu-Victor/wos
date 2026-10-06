#!/usr/bin/env python3
"""Receipt integrity and fixed-gate tests; no VM or device is launched."""

import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location("block_qualification", ROOT / "scripts/bench/run_block_qualification.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def report():
    lines = []
    for name, workers in (("qd1", 1), ("qd8", 8)):
        data = {"pass": True, "workers": workers, "bytes": module.BYTES_PER_FILE * workers, "block_bytes": module.BLOCK_BYTES,
                "pattern": module.PATTERN, "elapsed_ns": 1000000, "mib_per_second": 100}
        lines += [f"BLOCKCASE={name}", json.dumps(data), "BLOCKSTATUS=0"]
    for name in ("create", "rename"):
        lines += [f"BLOCKCASE={name}", json.dumps({"iterations": 1000, "workers": 1, "elapsed_seconds": 1}), "BLOCKSTATUS=0"]
    lines += ["BLOCKCASE=fsync", json.dumps({"pass": True, "operations": 100, "latencies_ns": [1000] * 100, "p95_ns": 1000}),
              "BLOCKSTATUS=0", "BLOCKDONE-failed=0"]
    return "\n".join(lines)


def expect_error(call):
    try:
        call()
    except (ValueError, KeyError):
        return
    raise AssertionError("invalid receipt was accepted")


def main():
    results, _ = module.parse_report(report())
    expect_error(lambda: module.parse_report(report().replace("BLOCKSTATUS=0", "BLOCKSTATUS=1", 1)))
    expect_error(lambda: module.parse_report(report() + "\nBLOCKCASE=qd1\n{}\nBLOCKSTATUS=0"))
    expect_error(lambda: module.parse_report(report().replace('"operations": 100', '"operations": 99')))
    expect_error(lambda: module.parse_report(report().replace('"p95_ns": 1000', '"p95_ns": 999')))
    spec = module.node_setup.normalize_node_spec({"vm": {"disk_cache": "writeback"}})
    assert spec["vm"]["disk_cache"] == "writeback"
    assert "sleep 0." not in module.guest_script()
    assert module.guest_script().count("BLOCKCASE=") == 5
    assert module.PROFILE["qd8_min_ratio"] == 1.15 and module.PROFILE["fsync_p95_max_ratio"] == 1.10
    serial = "system shutdown finalizer starting\nshutdown requested stop for 8 kernel threads\nahci: DMA retirement result=0 bme_disabled=1 regions=20->0 pages=2048->0 quarantined=0\n"
    assert module.shutdown_receipt(serial)["pages_after"] == 0
    expect_error(lambda: module.shutdown_receipt(serial.replace("pages=2048->0", "pages=2048->1")))
    expect_error(lambda: module.shutdown_receipt(serial + "shutdown continuing with 1 kernel threads still active\n"))
    expect_error(lambda: module.shutdown_receipt(serial + "KASAN: use-after-free\n"))

    scratch = ROOT / "build/goal30-tmp"
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="block-receipts-", dir=scratch) as temporary:
        # Compile actual init source: parsing a stanza as text misses validator
        # range/readiness/identity rules applied by the guest supervisor.
        validator = Path(temporary) / "manifest-validator"
        subprocess.run([os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++"), "-std=c++23", "-O1", "-g", "-fsanitize=address,undefined",
                        "-fno-omit-frame-pointer", "-I", str(ROOT / "modules/init/src"),
                        str(ROOT / "modules/init/src/service_manifest.cpp"),
                        str(ROOT / "tests/host/unit/service_manifest_validation.cpp"), "-o", str(validator)], check=True)
        child_environment = dict(os.environ, ASAN_OPTIONS="detect_leaks=0", LSAN_OPTIONS="detect_leaks=0")

        def validate_manifest(text):
            result = subprocess.run([str(validator)], input=text, text=True, capture_output=True,
                                    env=child_environment, timeout=10)
            return result.returncode, json.loads(result.stdout)

        service = module.manifest()
        code, parsed = validate_manifest(service)
        assert code == 0 and parsed["parse_error"] == 0 and parsed["validation_error"] == 0 and parsed["services"] == 2
        for invalid in (service.replace("readiness-timeout-ms=600000", "readiness-timeout-ms=600001"),
                        service.replace("readiness-timeout-ms=600000", "readiness-timeout-ms=900000")):
            code, parsed = validate_manifest(invalid)
            assert code == 2 and parsed["parse_error"] == 0 and parsed["validation_error"] == 7
        owner_service = service.replace("type=oneshot", "type=daemon").replace("readiness=exit-success", "readiness=immediate")
        assert validate_manifest(owner_service)[0] == 0
        for invalid in ("version=1\n[service block-qualification]" + service.split("[service block-qualification]", 1)[1].replace("depends=journald\n", ""),
                        service.replace("depends=journald\n", "")):
            code, parsed = validate_manifest(invalid)
            assert code == 2 and parsed["parse_error"] == 0 and parsed["validation_error"] == 15
        code, parsed = validate_manifest(service.replace("arg=/bin/sh", "arg=/bin/false"))
        assert code == 2 and parsed["validation_error"] == 5

        paths = [Path(temporary) / f"{index}.json" for index in range(10)]
        provenance = {"profile": module.PROFILE, "source_boot_sha256": "boot", "source_media_sha256": "media",
                      "media_provenance": {"testprog_sha256": "shared"}, "guest_sha256": "guest", "runner_sha256": "runner"}
        rows = []
        hardware = {"device": "disk", "depth": "8", "ahci_depth": "8", "ahci_ncq": "1", "hardware_high": "8",
                    "owned": "0", "owned_bytes": "0", "queued": "0", "inflight": "0", "inflight_bytes": "0",
                    "completing": "0", "timeouts": "0", "submitted": "10", "completed": "10", "released": "10",
                    "ahci_active": "0", "hardware_active": "0", "ahci_quarantines": "0", "ahci_offline": "0"}
        for index, path in enumerate(paths):
            row = {"completed": True, "errors": [], "provenance": dict(provenance, kernel_sha256="old" if index % 2 == 0 else "new"),
                   "results": copy.deepcopy(results), "queue_snapshots": [hardware] if index % 2 else [],
                   "cold_recovery": {"pass": True}, "shutdown": {"quarantined": 0}}
            if index % 2:
                row["results"]["qd8"]["data"]["mib_per_second"] = 116
            rows.append(row)
            path.write_text(json.dumps(row))
        assert module.compare(paths)["pass"]
        rows[1]["results"]["qd8"]["data"]["mib_per_second"] = 114
        rows[3]["results"]["qd8"]["data"]["mib_per_second"] = 114
        rows[5]["results"]["qd8"]["data"]["mib_per_second"] = 114
        for index in (1, 3, 5):
            paths[index].write_text(json.dumps(rows[index]))
        assert not module.compare(paths)["pass"]
        rows[1]["completed"] = False
        paths[1].write_text(json.dumps(rows[1]))
        assert not module.compare(paths)["pass"]
        rows[1]["completed"] = True
        rows[1]["provenance"]["guest_sha256"] = "changed"
        paths[1].write_text(json.dumps(rows[1]))
        expect_error(lambda: module.compare(paths))
        rows[1]["provenance"]["guest_sha256"] = "guest"
        rows[1]["queue_snapshots"] = [dict(hardware, hardware_high="7")]
        paths[1].write_text(json.dumps(rows[1]))
        expect_error(lambda: module.compare(paths))
    print("storage qualification receipts: actual init manifest validation, integrity, all ten runs, unchanged gates, actual QD8 and ownership PASS")


if __name__ == "__main__":
    main()
