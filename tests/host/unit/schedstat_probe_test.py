#!/usr/bin/env python3

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PROBE = ROOT / "scripts" / "bench" / "schedstat_probe.py"


def load_module():
    spec = importlib.util.spec_from_file_location("schedstat_probe", PROBE)
    if spec is None or spec.loader is None:
        raise AssertionError(f"failed to load {PROBE}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def schedstat_row(vcpu: int, run_ns: int) -> dict:
    return {
        "vcpu": vcpu,
        "tid": 1000 + vcpu,
        "comm": f"CPU {vcpu}/KVM",
        "psr": vcpu,
        "schedstat": (run_ns, 0, 1),
    }


def test_capacity_weighted_utilization(module) -> None:
    specs = [
        module.QemuSpec("vm0", 900001, "test"),
        module.QemuSpec("vm1", 900002, "test"),
    ]
    threads = {
        "vm0": [
            module.VcpuThread(0, 1000, "CPU 0/KVM"),
            module.VcpuThread(1, 1001, "CPU 1/KVM"),
        ],
        "vm1": [module.VcpuThread(0, 2000, "CPU 0/KVM")],
    }
    before = {
        "vm0": {
            "0": schedstat_row(0, 0),
            "1": schedstat_row(1, 0),
        },
        "vm1": {"0": schedstat_row(0, 0)},
    }
    after = {
        "vm0": {
            "0": schedstat_row(0, 800_000_000),
            "1": schedstat_row(1, 600_000_000),
        },
        "vm1": {"0": schedstat_row(0, 900_000_000)},
    }
    payload = {
        "mode": "duration",
        "duration_seconds": 1,
        "returncode": 0,
        "stdout": "",
        "stderr": "",
    }

    result = module.build_result(
        specs,
        threads,
        0,
        before,
        1_000_000_000,
        after,
        payload,
    )

    vm0, vm1 = result["qemu"]
    assert abs(vm0["utilization"]["utilization_pct"] - 70.0) < 0.001
    assert abs(vm1["utilization"]["utilization_pct"] - 90.0) < 0.001
    summary = result["summary"]
    assert abs(summary["aggregate_utilization_pct"] - (2300 / 30)) < 0.001
    assert abs(summary["min_vm_utilization_pct"] - 70.0) < 0.001
    assert abs(summary["max_vm_utilization_pct"] - 90.0) < 0.001
    assert abs(summary["normalized_utilization_spread_pct_points"] - 20.0) < 0.001
    assert "aggregate=76.67%" in module.render_text_report(result)


def main() -> None:
    module = load_module()
    test_capacity_weighted_utilization(module)
    print("1 schedstat_probe test passed")


if __name__ == "__main__":
    main()
