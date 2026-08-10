#!/usr/bin/env python3

from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
MANIFEST = ROOT / "configs" / "wos-services.conf"
MAKE_INITRAMFS = ROOT / "scripts" / "build" / "make_initramfs.sh"
ROOTFS_ALIASES = ROOT / "configs" / "rootfs" / "aliases.tsv"
ROOT_CMAKE = ROOT / "CMakeLists.txt"


def fail(message: str) -> None:
    raise AssertionError(message)


def parse_manifest() -> tuple[list[str], dict[str, dict[str, list[str]]]]:
    raw = MANIFEST.read_bytes()
    if len(raw) > 32 * 1024:
        fail("service manifest exceeds the parser's 32 KiB bound")
    if any(len(line) > 512 for line in raw.splitlines()):
        fail("service manifest contains a line beyond the parser's 512-byte bound")

    content = [
        line.strip()
        for line in raw.decode("ascii").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if not content or content[0] != "version=1":
        fail("service manifest must start with version=1")

    order: list[str] = []
    services: dict[str, dict[str, list[str]]] = {}
    current: dict[str, list[str]] | None = None
    for line in content[1:]:
        if line.startswith("[service ") and line.endswith("]"):
            name = line[len("[service ") : -1]
            if not name or name in services:
                fail(f"invalid or duplicate service section {name!r}")
            order.append(name)
            current = defaultdict(list)
            services[name] = current
            continue
        if current is None or "=" not in line:
            fail(f"invalid service manifest line {line!r}")
        key, value = line.split("=", 1)
        if not key or not value:
            fail(f"empty manifest key or value in {line!r}")
        current[key].append(value)

    return order, services


def expected_service(
    executable: str,
    arguments: list[str],
    dependencies: list[str],
    service_type: str,
    service_class: str,
    priority: str,
    readiness: str,
    readiness_timeout_ms: str,
    restart: str,
    stdio: str,
    enable: str,
    **conditional: list[str],
) -> dict[str, list[str]]:
    fields = {
        "exec": [executable],
        "arg": arguments,
        "type": [service_type],
        "class": [service_class],
        "priority": [priority],
        "environment": ["init"],
        "readiness": [readiness],
        "readiness-timeout-ms": [readiness_timeout_ms],
        "restart": [restart],
        "backoff-initial-ms": ["250"],
        "backoff-max-ms": ["30000"],
        "restart-budget": ["5"],
        "restart-window-ms": ["60000"],
        "stable-run-ms": ["60000"],
        "stop-term-ms": ["2000"],
        "stop-kill-ms": ["1000"],
        "drain-timeout-ms": ["2000"],
        "stdio": [stdio],
        "enable": [enable],
    }
    if dependencies:
        fields["depends"] = dependencies
    fields.update(conditional)
    return fields


def test_manifest_contract() -> None:
    order, services = parse_manifest()
    expected = {
        "journald": expected_service(
            "/sbin/journald",
            ["/sbin/journald"],
            [],
            "daemon",
            "journal",
            "10",
            "immediate",
            "5000",
            "never",
            "inherit",
            "always",
        ),
        "dropbear-keygen": expected_service(
            "/bin/dropbearkey",
            [
                "/bin/dropbearkey",
                "-t",
                "rsa",
                "-f",
                "/etc/dropbear/dropbear_rsa_host_key",
            ],
            ["journald"],
            "oneshot",
            "normal",
            "0",
            "exit-success",
            "30000",
            "on-failure",
            "journal",
            "path-missing",
            **{"enable-path": ["/etc/dropbear/dropbear_rsa_host_key"]},
        ),
        "netd": expected_service(
            "/sbin/netd",
            ["/sbin/netd"],
            ["journald"],
            "daemon",
            "network-provider",
            "0",
            "ipv4",
            "180000",
            "on-failure",
            "inherit",
            "always",
            **{"readiness-interface": ["eth0"]},
        ),
        "httpd": expected_service(
            "/sbin/httpd",
            ["/sbin/httpd"],
            ["netd"],
            "daemon",
            "network-consumer",
            "10",
            "immediate",
            "5000",
            "on-failure",
            "inherit",
            "always",
        ),
        "dropbear": expected_service(
            "/bin/dropbear",
            ["/bin/dropbear", "-r", "/etc/dropbear/dropbear_rsa_host_key", "-F"],
            ["netd", "dropbear-keygen"],
            "daemon",
            "network-consumer",
            "-5",
            "immediate",
            "5000",
            "on-failure",
            "journal",
            "always",
        ),
        "testd": expected_service(
            "/usr/bin/testd",
            ["/usr/bin/testd"],
            ["netd", "httpd", "dropbear"],
            "oneshot",
            "normal",
            "0",
            "exit-success",
            "5000",
            "never",
            "inherit",
            "disabled",
        ),
    }

    if order != list(expected):
        fail(f"unexpected service order: {order!r}")
    for name, fields in expected.items():
        actual = dict(services[name])
        if actual != fields:
            fail(f"{name} manifest mismatch:\nexpected {fields!r}\nactual   {actual!r}")
        if actual["arg"][0] != actual["exec"][0]:
            fail(f"{name} argv[0] must equal exec")


def test_packaging_contract() -> None:
    initramfs = MAKE_INITRAMFS.read_text(encoding="utf-8")
    for token in [
        'SERVICE_MANIFEST="$CWD/configs/wos-services.conf"',
        'cp "$SERVICE_MANIFEST" "$INITRAMFS_DIR/etc/wos-services.conf"',
        "initramfs: added /etc/wos-services.conf from configs/wos-services.conf",
    ]:
        if token not in initramfs:
            fail(f"initramfs packaging missing {token!r}")

    aliases = ROOTFS_ALIASES.read_text(encoding="utf-8")
    if "copy\tconfigs/wos-services.conf\t/etc/wos-services.conf" not in aliases:
        fail("normal rootfs packaging must install /etc/wos-services.conf")

    cmake = ROOT_CMAKE.read_text(encoding="utf-8")
    dependency = "${CMAKE_SOURCE_DIR}/configs/wos-services.conf"
    if cmake.count(dependency) != 2:
        fail("service manifest must invalidate both rootfs and boot-image outputs")


def main() -> None:
    test_manifest_contract()
    test_packaging_contract()
    print("default service manifest and packaging invariants hold")


if __name__ == "__main__":
    main()
