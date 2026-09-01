#!/usr/bin/env python3

import shutil
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DISSECTOR = ROOT / "tools" / "wireshark" / "wki.lua"


def require_tokens(source: str, tokens: list[str], contract: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        raise AssertionError(f"{contract}: missing {missing}")


def main() -> None:
    source = DISSECTOR.read_text()

    require_tokens(
        source,
        [
            "local WKI_VERSION = 3",
            "auth_trailer_size = 40",
            "max_payload = 8914",
            "hello_payload_size = 172",
            '[0x0C] = "PEER_GOODBYE"',
            '[0x0D] = "HELLO_CONFIRM"',
            '[0x56] = "TASK_SUBMIT_FRAGMENT"',
        ],
        "WKI v3 wire constants",
    )
    require_tokens(
        source,
        [
            'ProtoField.bytes("wki.auth.session_id", "Session ID")',
            'ProtoField.uint64("wki.auth.counter", "Session Counter", base.DEC)',
            'ProtoField.bytes("wki.auth.tag", "HMAC-SHA-256 Tag (128-bit)")',
            "local is_discovery_hint = version == WKI_VERSION and msg_type == 0x01 and dst_node == 0xFFFF",
            "local auth_expected = version == WKI_VERSION and not is_discovery_hint",
            "local expected_frame_len = payload_end + (auth_expected and WKI_V3.auth_trailer_size or 0)",
            "auth_buf(0, 16)",
            "auth_buf(16, 8)",
            "auth_buf(24, 16)",
            '"WKI v3 frame length mismatch: captured %u, expected exactly %u bytes"',
        ],
        "authentication trailer and discovery framing",
    )
    require_tokens(
        source,
        [
            "msg_type == 0x01 or msg_type == 0x02 or msg_type == 0x0D",
            "payload_buf(24, 4)",
            "payload_buf(28, 4)",
            "payload_buf(96, 2)",
            "payload_buf(98, 2)",
            "payload_buf(100, 32)",
            "payload_buf(132, 4)",
            "payload_buf(136, 32)",
            "payload_buf(168, 4)",
            '[6] = "ACCESS_DENIED"',
            '[5] = "UNAUTHORIZED"',
        ],
        "authenticated HELLO and authorization values",
    )
    require_tokens(
        source,
        [
            "msg_type ~= 0x0C and msg_type ~= 0x0D",
            '"Unsupported WKI version %d (secure-only dissector expects %d)"',
            '"WKI v3 authenticated protocol dissector loaded (EtherType 0x88B7) with statistics support"',
        ],
        "secure-only analysis behavior",
    )

    luac = shutil.which("luac")
    if luac is not None:
        subprocess.run([luac, "-p", str(DISSECTOR)], check=True)

    print("WKI Wireshark v3 dissector invariants hold")


if __name__ == "__main__":
    main()
