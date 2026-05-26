#!/usr/bin/env python3
"""
ktest_cov.py — Symbolize ktest KCOV PC dump into a coverage report.

Usage:
    python3 scripts/ktest_cov.py [options]

    -e / --elf     Kernel ELF  (default: build/modules/kern/wos)
    -l / --log     Serial log  (default: serial-vm0.log)
    --lcov FILE    Also write an lcov .info file (load with genhtml)
    --src PATH     Filter to source files containing PATH

Serial log lines look like (journal format):
    [T.mmm] INFO kcov_begin: <unique_count>
    [T.mmm] INFO kcov: 0x... 0x... 0x... 0x... 0x... 0x... 0x... 0x...
    [T.mmm] INFO kcov_end:

After generating an lcov file:
    genhtml coverage.info -o coverage-html/
    xdg-open coverage-html/index.html
"""

import argparse
import subprocess
import sys
from collections import defaultdict


def extract_pcs(log_path: str) -> list[int]:
    pcs = []
    in_block = False
    with open(log_path) as f:
        for line in f:
            if "kcov_begin:" in line:
                in_block = True
                continue
            if "kcov_end:" in line:
                break
            if in_block and " kcov: " in line:
                # message part follows "kcov: ", may contain 1–8 hex addresses
                msg = line.split(" kcov: ", 1)[1]
                for token in msg.split():
                    if token.startswith("0x"):
                        try:
                            pcs.append(int(token, 16))
                        except ValueError:
                            pass
    return pcs


def symbolize(pcs: list[int], elf: str) -> list[tuple[str, int]]:
    if not pcs:
        return []
    addr_input = "\n".join(f"0x{pc:x}" for pc in pcs)
    result = subprocess.run(
        ["llvm-addr2line", "-e", elf, "-f", "-i"],
        input=addr_input, capture_output=True, text=True,
    )
    locations = []
    lines = result.stdout.strip().splitlines()
    i = 0
    while i + 1 < len(lines):
        loc = lines[i + 1]  # skip function name on lines[i]
        i += 2
        if "?" in loc or ":" not in loc:
            continue
        file_part, _, line_part = loc.rpartition(":")
        try:
            locations.append((file_part, int(line_part)))
        except ValueError:
            pass
    return locations


def print_report(by_file: dict[str, set[int]], src_filter: str) -> None:
    filtered = {
        f: lines for f, lines in by_file.items()
        if not src_filter or src_filter in f
    }
    if not filtered:
        print("No coverage data matched (check --src filter).")
        return

    total = sum(len(v) for v in filtered.values())
    col = min(max((len(f) for f in filtered), default=4), 80)

    print(f"{'File':<{col}}  Lines hit")
    print("-" * (col + 12))
    for path in sorted(filtered):
        display = path
        if src_filter and src_filter in path:
            display = path[path.index(src_filter):]
        print(f"{display:<{col}}  {len(filtered[path])}")
    print("-" * (col + 12))
    print(f"Total: {total} unique lines across {len(filtered)} files")


def write_lcov(by_file: dict[str, set[int]], out_path: str) -> None:
    with open(out_path, "w") as f:
        for path in sorted(by_file):
            f.write(f"SF:{path}\n")
            for line in sorted(by_file[path]):
                f.write(f"DA:{line},1\n")
            f.write("end_of_record\n")
    print(f"\nlcov written to {out_path}")
    print(f"  genhtml {out_path} -o coverage-html/ && xdg-open coverage-html/index.html")


def main() -> None:
    ap = argparse.ArgumentParser(description="Symbolize ktest KCOV coverage dump")
    ap.add_argument("-e", "--elf", default="build/modules/kern/wos")
    ap.add_argument("-l", "--log", default="serial-vm0.log")
    ap.add_argument("--lcov", metavar="FILE", help="Write lcov .info file")
    ap.add_argument("--src", metavar="PATH", default="", help="Filter to files containing PATH")
    args = ap.parse_args()

    print(f"Reading PCs from {args.log} ...")
    pcs = extract_pcs(args.log)
    if not pcs:
        print("No kcov block found. Build with WOS_KCOV=ON + WOS_SELFTEST=ON and boot with --selftest.")
        sys.exit(1)

    unique_pcs = list(set(pcs))
    print(f"  {len(pcs)} total PCs, {len(unique_pcs)} unique")

    print(f"Symbolizing against {args.elf} ...")
    locs = symbolize(unique_pcs, args.elf)
    by_file: dict[str, set[int]] = defaultdict(set)
    for path, lineno in locs:
        by_file[path].add(lineno)
    print(f"  {sum(len(v) for v in by_file.values())} unique file:line pairs in {len(by_file)} files\n")

    print_report(by_file, args.src)

    if args.lcov:
        write_lcov(by_file, args.lcov)


if __name__ == "__main__":
    main()
