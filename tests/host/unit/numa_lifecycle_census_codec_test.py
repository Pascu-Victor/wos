#!/usr/bin/env python3
"""Exercise strict saved census decoding; synthetic controls grant no runtime credit."""

from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path
import struct
import tempfile

ROOT = Path(__file__).resolve().parents[3]
NONCE = 11


def load_decoder(path):
    spec = importlib.util.spec_from_file_location("census_codec", path)
    if spec is None or spec.loader is None:
        raise ValueError("decoder unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def report(phase):
    output = bytearray(46360)
    struct.pack_into(
        "<16Q", output, 0, 1, 46360, phase, 1, 1, NONCE, 255, 255, phase * 10, 900000010, phase, 0, 0, 2, 9, 0
    )
    struct.pack_into("<27Q", output, 128, 3, 3, 0, 1, *([0] * 23))
    for index in range(3):
        n = 2 if index == 0 else 1
        struct.pack_into(
            "<88Q",
            output,
            600 + index * 704,
            (1 << 64) - 1 if index == 0 else index - 1,
            n * 1000,
            n,
            n,
            n * 998,
            0,
            *([0] * 82),
        )
    return bytes(output)


def log():
    lines = [
        "NUMA_TOPOLOGY node=0 managed=4096000 cpus=000000000000000f:0000000000000000:0000000000000000:0000000000000000 eligible=1 reason=eligible",
        "NUMA_TOPOLOGY node=1 managed=4096000 cpus=00000000000000f0:0000000000000000:0000000000000000:0000000000000000 eligible=1 reason=eligible",
        "NUMA_PROFILE version=1 provenance=2 nodes=2 cpus=8 workers=2 free_capacity=not_observed policy=LOCAL",
        "NUMA_LIFECYCLE_PLACEMENT parent=0:0 child=controller_target_cpu method=inherited_ready_cooperative_placement",
    ]

    def own(phase, n):
        return (
            f"NUMA_OWN_PARENT phase={phase} cycles={n} fds=0 maps=0 max_fds={2 if n else 0} "
            f"max_maps={1 if n else 0} created_fds={n * 2} closed_fds={n * 2} created_maps={n} "
            f"unmapped={n} complete=1 source=exact_created_resources global_debt=not_observed"
        )

    lines.append(own("begin", 0))
    for cycle in range(1000):
        node = cycle % 2
        cpu = node * 4
        lines.append(
            f"NUMA_CHILD cycle={cycle} pid={100 + cycle} tid={100 + cycle} seed={NONCE} "
            f"cpu={cpu}:{cpu} target={node}:{cpu} nodes={node}:{node} policy=LOCAL elapsed_ns=100 "
            "wait0=1 unmapped=1 source_nodes=0:0"
        )
        if (cycle + 1) % 128 == 0 or cycle == 999:
            lines.append(own("settled", cycle + 1))
    lines.extend(
        [
            own("final", 1000),
            f"NUMA_LIFECYCLE_RESULT pass=1 completed=1000 requested=1000 seed={NONCE} "
            "parent_cpu=0 same_node=500 cross_node=500 cross_node_cow=500 cow_local=2000 cow_resident=2000 "
            "cow_local_percent_min=95 scope=map_fork_cow_crosspage_pipe_wait_unmap_only",
        ]
    )
    return ("\n".join(lines) + "\n").encode()


def physical_reports(codec, directory, original, good):
    """Exercise ownership closure independently of the free-page cache partition."""
    checks = 0

    def word(data, row, field):
        return struct.unpack_from("<Q", data, 600 + row * 704 + field * 8)[0]

    def changed(data, updates):
        result = bytearray(data)
        for row, field, value in updates:
            struct.pack_into("<Q", result, 600 + row * 704 + field * 8, value)
        return bytes(result)

    def pair(begin_updates, end_updates, label, reason=None, reports_valid=True):
        nonlocal checks
        begin = changed(original["begin.bin"], begin_updates)
        end = changed(original["end.bin"], end_updates)
        assert (begin, end) != (original["begin.bin"], original["end.bin"]), "control must change actual bytes"
        (directory / "begin.bin").write_bytes(begin)
        (directory / "end.bin").write_bytes(end)
        try:
            if reports_valid:
                codec.report(directory / "begin.bin", 1, NONCE, 2)
                codec.report(directory / "end.bin", 2, NONCE, 2)
            try:
                decoded = codec.decode(directory, NONCE, 2)
            except (ValueError, UnicodeError) as error:
                if reason is None:
                    raise AssertionError(f"cache-only saved report acceptance: {label}") from error
                assert str(error) == reason, f"wrong rejection for {label}: {error}"
            else:
                assert reason is None, f"accepted physical ownership drift: {label}"
                assert decoded == good, f"cache-only saved report changed decoded qualification: {label}"
            checks += 1
        finally:
            for name in ["begin.bin", "end.bin"]:
                (directory / name).write_bytes(original[name])

    # Each report reconciles global cached pages to its two active nodes.
    # These are historical BEGIN/END changes; adjacent kernel quiet samples
    # retain their separate cache-exact predicate.
    for label, begin, end in [
        ("decrease", (56, 28, 28), (50, 25, 25)),
        ("increase", (40, 20, 20), (58, 29, 29)),
        ("per-node repartition", (56, 28, 28), (56, 25, 31)),
    ]:
        pair(
            [(row, 5, value) for row, value in enumerate(begin)],
            [(row, 5, value) for row, value in enumerate(end)],
            label,
        )

    invalid = "invalid tracked owner/free identity"
    closure = "exact MM/owner/physical baseline closure"
    for name in ["begin.bin", "end.bin"]:
        for row in range(3):
            updates = [(row, 5, word(original[name], row, 4) + 1)]
            if row != 0:
                # Keep global/node reconciliation coherent while the selected
                # node alone violates cached<=inclusive-free.
                updates.append((0, 5, updates[0][2]))
            pair(
                updates if name == "begin.bin" else [],
                updates if name == "end.bin" else [],
                f"{name} cached above inclusive free, row={row}",
                reason=invalid,
                reports_valid=False,
            )

    for node_row in [1, 2]:
        for owner in range(41):
            for kind, field in [("pages", 6 + owner * 2), ("objects", 7 + owner * 2)]:
                updates = []
                for row in [0, node_row]:
                    updates.append((row, field, word(original["end.bin"], row, field) + 1))
                    if kind == "pages":
                        updates.append((row, 4, word(original["end.bin"], row, 4) - 1))
                pair(
                    [],
                    updates,
                    f"owner={owner} {kind} global/node={node_row - 1}",
                    reason=invalid if owner == 0 else closure,
                    reports_valid=owner != 0,
                )

        # Complete per-report physical equations still hold for these changes;
        # only comparing BEGIN with END exposes their retained ownership/capacity.
        for label, deltas in [
            ("retained page", [(8, 1), (9, 1), (4, -1)]),
            ("inclusive free loss", [(1, -1), (4, -1)]),
            ("managed/free growth", [(1, 1), (4, 1)]),
            ("metadata growth", [(2, 1), (4, -1)]),
            ("descriptor growth", [(3, 1), (4, -1)]),
        ]:
            updates = [
                (row, field, word(original["end.bin"], row, field) + delta)
                for row in [0, node_row]
                for field, delta in deltas
            ]
            pair([], updates, f"{label} global/node={node_row - 1}", reason=closure)

    for name in ["begin.bin", "end.bin"]:
        updates = [(3, 5, 1)]
        pair(
            updates if name == "begin.bin" else [],
            updates if name == "end.bin" else [],
            f"{name} unused node cache",
            reason="nonzero unused physical rows",
            reports_valid=False,
        )
    return checks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--decoder", type=Path)
    args = parser.parse_args()
    codec = load_decoder(args.decoder or args.root / "tests/fixtures/validate_numa_lifecycle_census.py")
    checks = 0
    with tempfile.TemporaryDirectory(prefix="wos-census-codec-") as temporary:
        directory = Path(temporary)
        original = {"begin.bin": report(1), "end.bin": report(2)}
        for name in ["warm.log", "measured.log"]:
            original[name] = log().ljust(1024 * 1024, b"\0")
        text = (
            f"NUMA_CENSUS_WITNESS version=1 nonce={NONCE} cookie=1 pid=2 warm_pid=3 warm_cycles=1000 warm_wait0=1 "
            "measured_pid=4 measured_cycles=1000 measured_wait0=1 kernel_coverage=255 external_work_wait=1 combined_coverage=511\n"
        )
        original["witness.txt"] = text.encode().ljust(4096, b"\0")
        for name, data in original.items():
            (directory / name).write_bytes(data)
        good = codec.decode(directory, NONCE, 2)
        assert good["measured"]["cycles"] == 1000 and good["measured"]["actual_cross_node_cow"] == 500
        checks += 1

        def reject(name, data):
            nonlocal checks
            path = directory / name
            assert bytes(data) != original[name], "negative must change actual bytes"
            path.write_bytes(data)
            try:
                try:
                    codec.decode(directory, NONCE, 2)
                except (ValueError, UnicodeError):
                    checks += 1
                else:
                    raise AssertionError(f"accepted malformed saved data: {name}")
            finally:
                path.write_bytes(original[name])

        for index in [0, 1, 2, 3, 4, 5, 6, 7, 9, 12, 13, 14, 15]:
            data = bytearray(original["end.bin"])
            struct.pack_into("<Q", data, index * 8, 7 if index in [12, 15] else 0 if index != 3 else 2)
            reject("end.bin", data)
        for offset in [
            128,
            144,
            152,
            *[160 + i * 8 for i in [0, 1, 2, 3, 4, 5, 6, 12, 15, 16, 17, 18, 19, 22]],
            344,
            344 + 24 * 8,
            600 + 8,
            600 + 48,
            600 + 3 * 704,
        ]:
            data = bytearray(original["end.bin"])
            struct.pack_into("<Q", data, offset, 7)
            reject("end.bin", data)
        for name, data in original.items():
            reject(name, data[:-1])
            reject(name, data + b"\0")
        for old, new in [
            (b"child=controller_target_cpu method=inherited_ready_cooperative_placement", b"child=observed_current_cpu method=child_same_owner_pin"),
            (b"method=inherited_ready_cooperative_placement", b"method=unspecified_placement"),
            (b"cycle=500 ", b"cycle=499 "),
            (b"pid=100 tid=100 ", b"pid=100 tid=101 "),
            (b"seed=11 ", b"seed=12 "),
            (b"wait0=1", b"wait0=0"),
            (b"unmapped=1", b"unmapped=0"),
            (b"pass=1 completed=1000", b"pass=0 completed=1000"),
            (b"cow_local=2000", b"cow_local=1999"),
            (b"global_debt=not_observed", b"global_debt=proven"),
            (b"fds=0 maps=0", b"fds=1 maps=0"),
            (b"cpu=0:0", b"cpu=0:4"),
            (b"phase=final cycles=1000", b"phase=final cycles=999"),
            (b"\n", b"\r\n"),
            (b"\n", b"\xc2\x85"),
            (b"\n", b"\xe2\x80\xa8"),
        ]:
            data = original["measured.log"]
            assert old in data
            changed = data.replace(old, new, 1).rstrip(b"\0")
            reject("measured.log", changed.ljust(1024 * 1024, b"\0"))
        reject("measured.log", original["measured.log"][:-1] + b"x")
        reject("witness.txt", original["witness.txt"].replace(b"warm_wait0=1", b"warm_wait0=0"))
        reject("witness.txt", original["witness.txt"].replace(b"measured_pid=4", b"measured_pid=3"))
        reject("witness.txt", b"NUMA_CENSUS_FAILURE version=1 nonce=11 exit=74 phase=warm stage=fork\n".ljust(4096, b"\0"))
        checks += physical_reports(codec, directory, original, good)
        assert codec.decode(directory, NONCE, 2) == good
        checks += 1
    print(f"PASS strict saved census codec checks={checks}; synthetic controls only; no runtime qualification")


if __name__ == "__main__":
    main()
