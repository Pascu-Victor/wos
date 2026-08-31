#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import os
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
RESOLVER = ROOT / "scripts" / "remote" / "wos_resolve.py"


def load_resolver():
    spec = importlib.util.spec_from_file_location("wos_resolve", RESOLVER)
    if spec is None or spec.loader is None:
        raise AssertionError("unable to load wos_resolve.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> None:
    resolver = load_resolver()
    with tempfile.TemporaryDirectory(prefix="wos-resolve-test-") as directory:
        root = Path(directory)
        resolver.ROOT = root
        (root / "serial-vm0.log").write_text(
            "hostname='wos-0'\neth0 configured: ip=10.10.0.235 mask=255.255.255.0\n",
            encoding="utf-8",
        )
        (root / "serial-vm1.log").write_text(
            "hostname='wos-1'\neth0 configured with IP 10.10.0.236\n",
            encoding="utf-8",
        )
        (root / "ktest-data").mkdir()
        (root / "ktest-data" / "serial-vm0.log").write_text(
            "hostname='wos-ktest'\neth0 configured: ip=10.10.0.237 mask=255.255.255.0\n",
            encoding="utf-8",
        )
        os.utime(root / "serial-vm0.log", ns=(1, 1))
        os.utime(root / "serial-vm1.log", ns=(1, 1))
        os.utime(root / "ktest-data" / "serial-vm0.log", ns=(2, 2))
        chaos = root / "wki-chaos-data"
        chaos.mkdir()
        stale = chaos / "ethernet-serial-vm0.log"
        stale.write_text(
            "hostname='wos-0'\neth0 configured with IP 10.10.0.238\n",
            encoding="utf-8",
        )
        newest = chaos / "routed-serial-vm0.log"
        newest.write_text(
            "hostname='wos-0'\neth0 configured with IP 10.10.0.239\n",
            encoding="utf-8",
        )
        incomplete = chaos / "newer-incomplete-serial-vm0.log"
        incomplete.write_text("hostname='wos-0'\nboot still in progress\n", encoding="utf-8")
        os.utime(stale, ns=(1, 1))
        os.utime(newest, ns=(3, 3))
        os.utime(incomplete, ns=(4, 4))

        expected = {
            "vm0": "10.10.0.239",
            "wos-0": "10.10.0.239",
            "wos-0.wos": "10.10.0.239",
            "vm1": "10.10.0.236",
            "wos-1": "10.10.0.236",
            "wos-ktest": "10.10.0.237",
            "wos-ktest.wos": "10.10.0.237",
            "10.10.0.99": "10.10.0.99",
            "unknown": "unknown",
        }
        for target, address in expected.items():
            actual = resolver.resolve_target(target)
            if actual != address:
                raise AssertionError(f"{target!r}: expected {address!r}, got {actual!r}")

        if resolver.resolve_path("/wki/vm0/tmp/file") != "/wki/wos-0/tmp/file":
            raise AssertionError("VM path aliases must still resolve through the matching hostname")

        bounded = chaos / "bounded-serial-vm2.log"
        bounded.write_bytes(
            b"hostname='obsolete'\neth0 configured with IP 10.1.1.1\n"
            + b"x" * (resolver.MAX_LOG_TAIL_BYTES + 64)
            + b"\nhostname='wos-2'\neth0 configured with IP 10.10.0.240\n"
        )
        os.utime(bounded, ns=(5, 5))
        if resolver.resolve_target("wos-2") != "10.10.0.240":
            raise AssertionError("bounded log tail did not retain the newest complete observation")

    print("WOS resolver selects bounded newest complete cluster, KTEST, and chaos log observations")


if __name__ == "__main__":
    main()
