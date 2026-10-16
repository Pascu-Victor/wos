#!/usr/bin/env python3
"""Strict saved guest census/probe decoder; host shutdown/storage proof is separate."""

from __future__ import annotations
import argparse
import json
from pathlib import Path
import re
import struct
import stat

REPORT_BYTES = 46360
LOG_BYTES = 1024 * 1024
MAX_NODES = 64
OWNERS = 41
MM_FIELDS = 23
HEADER = struct.Struct("<16Q")
U64 = re.compile(r"(0|[1-9][0-9]*)\Z", re.ASCII)


def require(value, why):
    if not value:
        raise ValueError(why)


def number(text):
    require(bool(U64.fullmatch(text)), "noncanonical decimal")
    value = int(text)
    require(value < 1 << 64, "uint64 overflow")
    return value


def fixed_bytes(path, expected_bytes):
    path = Path(path)
    before = path.lstat()
    require(
        stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size == expected_bytes,
        "expected exact regular fixed file",
    )
    data = path.read_bytes()
    after = path.lstat()
    require(
        (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
        == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns),
        "saved file changed while decoding",
    )
    require(len(data) == expected_bytes, "wrong fixed transport file size")
    return data


def fixed_text(path, expected_bytes):
    data = fixed_bytes(path, expected_bytes)
    end = data.find(b"\0")
    if end >= 0:
        require(not any(data[end:]), "nonzero bytes after fixed log terminator")
        data = data[:end]
    require(data.endswith(b"\n") and b"\r" not in data, "expected complete LF-only text")
    text = data.decode("ascii", errors="strict")
    require(all(ch == "\n" or 32 <= ord(ch) <= 126 for ch in text), "non-ASCII/control text")
    return text.split("\n")[:-1]


def full(pattern, line, why):
    result = re.fullmatch(pattern, line, flags=re.ASCII)
    require(result is not None, why)
    return result.groups()


def report(path, phase, nonce, nodes):
    data = fixed_bytes(path, REPORT_BYTES)
    h = HEADER.unpack_from(data)
    (
        version,
        size,
        actual_phase,
        outcome,
        cookie,
        actual_nonce,
        coverage,
        required,
        capture,
        deadline,
        calls,
        waits,
        reason,
        count,
        root,
        reserved,
    ) = h
    require(
        (version, size, actual_phase, outcome, actual_nonce, coverage, required, reason, count, reserved)
        == (1, REPORT_BYTES, phase, 1, nonce, 255, 255, 0, nodes, 0),
        "unqualified report header",
    )
    require(cookie > 0 and root > 0 and 0 < capture < deadline and calls > waits, "invalid session/time/header counts")
    mm = struct.unpack_from("<27Q", data, 128)
    require(mm[0] == mm[1] and mm[0] > 0 and mm[2:4] == (0, 1), "unstable/invalid MM stamp")
    debt = mm[4:]
    require(len(debt) == MM_FIELDS, "wrong MM field count")
    for index in [*range(7), 12, 15, 16, 17, 18, 19, 22]:
        require(debt[index] == 0, "live MM operation/pin/return debt")
    totals = struct.unpack_from("<32Q", data, 344)
    require(
        totals[-1] == 0 and all(totals[i] == 0 for i in [24, 25, 26, 27, 28, 29, 30]),
        "transient provider/FD/recovery debt",
    )
    physical = [struct.unpack_from("<88Q", data, 600 + i * 704) for i in range(65)]
    for i, row in enumerate(physical[: nodes + 1]):
        node, managed, metadata, descriptor, free, cached = row[:6]
        require(node == ((1 << 64) - 1 if i == 0 else i - 1), "physical row identity")
        require(
            (i != 0 or managed > 0) and cached <= free and row[6:8] == (0, 0), "invalid tracked owner/free identity"
        )
        owner_pages = sum(row[6 + owner * 2] for owner in range(OWNERS))
        require(managed == metadata + descriptor + free + owner_pages, "physical owner reconciliation")
    require(all(not any(row) for row in physical[nodes + 1 :]), "nonzero unused physical rows")
    for index in range(1, 88):
        require(
            physical[0][index] == sum(row[index] for row in physical[1 : nodes + 1]),
            "global/node exact physical reconciliation",
        )
    return dict(header=h, mm=mm, totals=totals, physical=physical)


CHILD = (
    r"NUMA_CHILD cycle=(\d+) pid=(\d+) tid=(\d+) seed=(\d+) cpu=(\d+):(\d+) target=(\d+):(\d+) "
    r"nodes=(\d+):(\d+) policy=LOCAL elapsed_ns=(\d+) wait0=1 unmapped=1 source_nodes=(\d+):(\d+)"
)


def probe(path, nonce, node_count):
    lines = fixed_text(path, LOG_BYTES)
    topology = {}
    children = []
    own = []
    profile = placement = summary = None
    for line in lines:
        if line.startswith("NUMA_TOPOLOGY "):
            node, managed, *rest = full(
                r"NUMA_TOPOLOGY node=(\d+) managed=(\d+) cpus=([0-9a-f]{16}):([0-9a-f]{16}):([0-9a-f]{16}):([0-9a-f]{16}) eligible=([01]) reason=([a-z0-9_]+)",
                line,
                "topology syntax",
            )
            node = number(node)
            require(node not in topology, "duplicate topology row")
            topology[node] = (number(managed), [int(word, 16) for word in rest[:4]], number(rest[4]), rest[5])
        elif line.startswith("NUMA_PROFILE "):
            require(profile is None, "duplicate profile")
            profile = full(
                r"NUMA_PROFILE version=1 provenance=(\d+) nodes=(\d+) cpus=8 workers=(\d+) free_capacity=not_observed policy=LOCAL",
                line,
                "profile syntax",
            )
        elif line.startswith("NUMA_LIFECYCLE_PLACEMENT "):
            require(placement is None, "duplicate placement")
            placement = tuple(
                map(
                    number,
                    full(
                        r"NUMA_LIFECYCLE_PLACEMENT parent=(\d+):(\d+) child=controller_target_cpu method=inherited_ready_cooperative_placement",
                        line,
                        "placement syntax",
                    ),
                )
            )
        elif line.startswith("NUMA_OWN_PARENT "):
            values = full(
                r"NUMA_OWN_PARENT phase=(begin|settled|final) cycles=(\d+) fds=(\d+) maps=(\d+) max_fds=(\d+) max_maps=(\d+) created_fds=(\d+) closed_fds=(\d+) created_maps=(\d+) unmapped=(\d+) complete=1 source=exact_created_resources global_debt=not_observed",
                line,
                "own-resource syntax",
            )
            own.append((values[0], *map(number, values[1:])))
        elif line.startswith("NUMA_CHILD "):
            children.append(tuple(map(number, full(CHILD, line, "child cycle syntax"))))
        elif line.startswith("NUMA_LIFECYCLE_RESULT "):
            require(summary is None, "duplicate lifecycle result")
            summary = tuple(
                map(
                    number,
                    full(
                        r"NUMA_LIFECYCLE_RESULT pass=1 completed=1000 requested=1000 seed=(\d+) parent_cpu=(\d+) same_node=(\d+) cross_node=(\d+) cross_node_cow=(\d+) cow_local=(\d+) cow_resident=2000 cow_local_percent_min=95 scope=map_fork_cow_crosspage_pipe_wait_unmap_only",
                        line,
                        "complete lifecycle result syntax",
                    ),
                )
            )
        else:
            raise ValueError("unknown/error probe line")
    require(
        set(topology) == set(range(node_count))
        and profile is not None
        and placement is not None
        and summary is not None,
        "complete profile/placement/topology/result required",
    )
    require(number(profile[0]) <= 3 and number(profile[1]) == node_count, "profile geometry/provenance")
    all_cpus = 0
    for managed, words, eligible, reason in topology.values():
        mask = sum(word << (64 * i) for i, word in enumerate(words))
        require(mask < 256 and not (all_cpus & mask), "CPU masks overlap/exceed eight")
        all_cpus |= mask
        if eligible:
            require(managed > 0 and mask != 0 and reason == "eligible", "eligible topology predicate")
    require(number(profile[2]) == min(2, sum(value[2] for value in topology.values())), "actual eligible worker count")
    require(
        all_cpus == 255 and len(children) == 1000 and [r[0] for r in children] == list(range(1000)),
        "exact CPU/cycle coverage",
    )
    parent_node, parent_cpu = placement
    require(
        parent_cpu < 8 and parent_node in topology and topology[parent_node][1][0] & (1 << parent_cpu),
        "parent actual CPU/node",
    )
    same = cross = cross_cow = local = 0
    for cycle, pid, tid, seed, before, after, node, cpu, first, second, elapsed, source1, source2 in children:
        require(
            pid > 1 and tid == pid and seed == nonce and elapsed > 0 and before == after == cpu and cpu < 8,
            "actual child identity/CPU/time",
        )
        require(
            node in topology and topology[node][0] > 0 and topology[node][1][0] & (1 << cpu), "child actual CPU/domain"
        )
        require(
            first in topology and second in topology and source1 == source2 == parent_node, "resident COW/source nodes"
        )
        same += node == parent_node
        cross += node != parent_node
        cross_cow += node != parent_node and first != source1 and second != source2
        local += (first == node) + (second == node)
    require(
        summary == (nonce, parent_cpu, same, cross, cross_cow, local) and local * 100 >= 2000 * 95,
        "summary must reproduce actual cycle locality",
    )
    require(node_count == 1 or (cross > 0 and cross_cow > 0), "NUMA cross-domain COW contrast missing")
    expected = [("begin", 0, 0, 0, 0, 0, 0, 0, 0, 0)]
    for n in [128, 256, 384, 512, 640, 768, 896, 1000]:
        expected.append(("settled", n, 0, 0, 2, 1, n * 2, n * 2, n, n))
    expected.append(("final", 1000, 0, 0, 2, 1, 2000, 2000, 1000, 1000))
    require(own == expected, "exact own FD/map created/closed/unmapped sequence")
    return dict(
        cycles=1000,
        actual_child_wait0=1000,
        same_node=same,
        cross_node=cross,
        actual_cross_node_cow=cross_cow,
        cow_resident=2000,
        cow_local=local,
        own_fds=0,
        own_maps=0,
        topology_nodes=node_count,
    )


def decode(directory, nonce, nodes):
    directory = Path(directory)
    require(0 < nonce < 1 << 64 and 1 <= nodes <= MAX_NODES, "bound nonce/topology")
    begin = report(directory / "begin.bin", 1, nonce, nodes)
    end = report(directory / "end.bin", 2, nonce, nodes)
    a, b = begin["header"], end["header"]
    require(
        a[4:8] == b[4:8] and a[9] == b[9] and a[14] == b[14] and a[8] < b[8] and a[10] < b[10] and a[11] <= b[11],
        "exact same session/root with ordered boundaries",
    )
    require(
        begin["mm"][4:] == end["mm"][4:] and begin["totals"] == end["totals"] and begin["physical"] == end["physical"],
        "exact MM/owner/physical baseline closure",
    )
    witness = fixed_text(directory / "witness.txt", 4096)
    require(len(witness) == 1, "one exact witness record")
    fields = tuple(
        map(
            number,
            full(
                r"NUMA_CENSUS_WITNESS version=1 nonce=(\d+) cookie=(\d+) pid=(\d+) warm_pid=(\d+) warm_cycles=1000 warm_wait0=1 measured_pid=(\d+) measured_cycles=1000 measured_wait0=1 kernel_coverage=255 external_work_wait=1 combined_coverage=511",
                witness[0],
                "witness consuming-wait syntax",
            ),
        )
    )
    require(
        fields[0] == nonce and fields[1] == a[4] and min(fields[2:]) > 1 and len(set(fields[2:])) == 3,
        "witness exact session and consuming children",
    )
    warm = probe(directory / "warm.log", nonce, nodes)
    measured = probe(directory / "measured.log", nonce, nodes)
    return dict(
        qualification="DECODED_GUEST_CENSUS_AND_TWO_REAL1000_PROBE_REPORTS_ONLY",
        nonce=nonce,
        cookie=a[4],
        root_incarnation=a[14],
        kernel_coverage=255,
        external_work_wait=1,
        combined_coverage=511,
        warm=warm,
        measured=measured,
        exact_23_mm_fields=23,
        exact_physical_rows=nodes + 1,
        init_witness_wait_and_natural_host_shutdown="REQUIRED_SEPARATE_HOST_EVIDENCE",
        stopped_readback_and_physical_zero="REQUIRED_SEPARATE_HOST_EVIDENCE",
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports", type=Path, required=True)
    parser.add_argument("--nonce", type=int, required=True)
    parser.add_argument("--nodes", type=int, required=True)
    args = parser.parse_args()
    print(json.dumps(decode(args.reports, args.nonce, args.nodes), indent=2))


if __name__ == "__main__":
    main()
