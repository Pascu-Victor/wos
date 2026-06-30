#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
ROOTFS_COMMON = ROOT / "scripts" / "build" / "rootfs_common.sh"


def fail(message: str) -> None:
    raise AssertionError(message)


def main() -> None:
    source = ROOTFS_COMMON.read_text(encoding="utf-8")
    if "root:!:0:0:root:/root:/bin/bash" not in source:
        fail("root passwd entry must be locked locally instead of forcing shadow lookup")
    if "root:x:0:0:root:/root:/bin/bash" in source:
        fail("root passwd entry must not force /etc/shadow lookup")
    for token in [
        "cat > \"$ROOTFS_STAGING/etc/shells\"",
        "/bin/bash",
        "/usr/bin/bash",
        "cp \"$ssh_pubkey\" \"$ROOTFS_STAGING/root/.ssh/authorized_keys\"",
        "chmod 600 \"$ROOTFS_STAGING/root/.ssh/authorized_keys\"",
    ]:
        if token not in source:
            fail(f"rootfs auth staging missing {token!r}")

    print("rootfs auth source invariants hold")


if __name__ == "__main__":
    main()
