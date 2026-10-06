#!/usr/bin/env python3
"""Generate a test translation unit fragment from complete XFS log functions.

Storage/flush are the only controlled I/O seams. The host target compiles the
real on-disk formats and CRC codec. No production clean/write logic is copied
into a test implementation.
"""
import argparse
from pathlib import Path

from exec_trace_ownership_test import function

ROOT = Path(__file__).resolve().parents[3]
NAMES = (
    "xfs_log_blocks_per_fs_block",
    "xfs_log_make_lsn",
    "xfs_log_next_clean_cycle",
    "xfs_log_record_fs_blocks",
    "xfs_log_advance_head",
    "xfs_log_device_span",
    "xfs_log_write_image",
    "xfs_log_write_record",
    "xfs_log_erase_device",
    "xfs_log_clear_clean",
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--clear-source", type=Path,
                        help="Optional historical source for negative clean-path evidence")
    args = parser.parse_args()
    source = (ROOT / "modules/kern/src/vfs/fs/xfs/xfs_log.cpp").read_text()
    historical = args.clear_source.read_text() if args.clear_source else source
    definitions = [function(historical if name == "xfs_log_clear_clean" else source, name)
                   for name in NAMES]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("// Generated from complete production function bodies.\n"
                           "namespace ker::vfs::xfs {\n" + "\n\n".join(definitions) + "\n}\n")


if __name__ == "__main__":
    main()
