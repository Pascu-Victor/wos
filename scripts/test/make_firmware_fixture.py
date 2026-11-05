#!/usr/bin/env python3
"""Write the small redistributable newc fixture for --selftest-firmware.

This contains WOS test bytes, never proprietary vendor firmware. The independent
kernel policy pins their name/length/SHA256/revision in firmware_runtime_ktest.
"""
import argparse
from pathlib import Path

PAYLOAD = b"WOS goal41 firmware fixture v1\n"


def record(name, payload=b"", mode=0o40755, links=2):
    name = name.encode("ascii") + b"\0"
    fields = (1, mode, 0, 0, links, 0, len(payload), 0, 0, 0, 0, len(name), 0)
    output = b"070701" + b"".join(f"{field:08x}".encode() for field in fields) + name
    output += bytes(-len(output) % 4)
    output += payload
    return output + bytes(-len(payload) % 4)


def fixture():
    output = b"".join(record(name) for name in (".", "lib", "lib/firmware", "lib/firmware/wos"))
    output += record("lib/firmware/wos/goal41.bin", PAYLOAD, 0o100444, 1)
    output += record("TRAILER!!!", mode=0, links=1)
    return output + bytes(-len(output) % 512)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    arguments = parser.parse_args()
    # Never replace an existing artifact silently.
    with arguments.output.open("xb") as output:
        output.write(fixture())
