#!/usr/bin/env python3

import argparse
import struct
from dataclasses import dataclass
from pathlib import Path


COREDUMP_MAGIC = 0x504D55444F43534F  # "WOSCODMP" little-endian-ish


@dataclass
class InterruptFrame:
    intNum: int
    errCode: int
    rip: int
    cs: int
    rflags: int
    rsp: int
    ss: int


@dataclass
class GPRegs:
    r15: int
    r14: int
    r13: int
    r12: int
    r11: int
    r10: int
    r9: int
    r8: int
    rbp: int
    rdi: int
    rsi: int
    rdx: int
    rcx: int
    rbx: int
    rax: int


def u64(x: int) -> str:
    return f"0x{x:016x}"


def parse_interrupt_frame(buf: bytes, off: int) -> tuple[InterruptFrame, int]:
    # struct interruptFrame packed: 7x u64
    (intNum, errCode, rip, cs, flags, rsp, ss) = struct.unpack_from("<7Q", buf, off)
    return InterruptFrame(intNum, errCode, rip, cs, flags, rsp, ss), off + 7 * 8


def parse_gpregs(buf: bytes, off: int) -> tuple[GPRegs, int]:
    # struct GPRegs packed: 15x u64
    vals = struct.unpack_from("<15Q", buf, off)
    return GPRegs(*vals), off + 15 * 8


def main() -> int:
    ap = argparse.ArgumentParser(description="Parse WOS core dump binary")
    ap.add_argument("file", type=Path)
    args = ap.parse_args()

    data = args.file.read_bytes()

    # Header prefix up to trapFrame/trapRegs/savedFrame/savedRegs is fixed.
    # Layout from modules/kern/src/platform/dbg/coredump.cpp CoreDumpHeader.
    off = 0
    (magic, version, headerSize) = struct.unpack_from("<QII", data, off)
    off += 8 + 4 + 4

    if magic != COREDUMP_MAGIC:
        raise SystemExit(f"Bad magic: {u64(magic)} (expected {u64(COREDUMP_MAGIC)})")

    (
        timestamp,
        pid,
        cpu,
        intNum,
        errCode,
        cr2,
        cr3,
    ) = struct.unpack_from("<7Q", data, off)
    off += 7 * 8

    trapFrame, off = parse_interrupt_frame(data, off)
    trapRegs, off = parse_gpregs(data, off)
    savedFrame, off = parse_interrupt_frame(data, off)
    savedRegs, off = parse_gpregs(data, off)

    (
        taskEntry,
        taskPagemap,
        elfHeaderAddr,
        programHeaderAddr,
        segmentCount,
        segmentTableOffset,
        elfSize,
        elfOffset,
    ) = struct.unpack_from("<8Q", data, off)
    off += 8 * 8

    print(f"file: {args.file}")
    print(f"magic: {u64(magic)} version: {version} headerSize: {headerSize}")
    print(f"timestampQuantums: {timestamp}")
    print(f"pid: {pid} cpu: {cpu}")
    print(f"intNum: {intNum} errCode: {errCode}")
    print(f"cr2: {u64(cr2)} cr3: {u64(cr3)}")
    print("\ntrapFrame:")
    print(f"  rip={u64(trapFrame.rip)} cs={u64(trapFrame.cs)} rflags={u64(trapFrame.rflags)} rsp={u64(trapFrame.rsp)} ss={u64(trapFrame.ss)}")
    print("trapRegs:")
    print(f"  rax={u64(trapRegs.rax)} rbx={u64(trapRegs.rbx)} rcx={u64(trapRegs.rcx)} rdx={u64(trapRegs.rdx)}")
    print(f"  rsi={u64(trapRegs.rsi)} rdi={u64(trapRegs.rdi)} rbp={u64(trapRegs.rbp)}")
    print(f"  r8={u64(trapRegs.r8)} r9={u64(trapRegs.r9)} r10={u64(trapRegs.r10)} r11={u64(trapRegs.r11)}")
    print(f"  r12={u64(trapRegs.r12)} r13={u64(trapRegs.r13)} r14={u64(trapRegs.r14)} r15={u64(trapRegs.r15)}")

    print("\nsavedFrame:")
    print(f"  rip={u64(savedFrame.rip)} cs={u64(savedFrame.cs)} rflags={u64(savedFrame.rflags)} rsp={u64(savedFrame.rsp)} ss={u64(savedFrame.ss)}")
    print("savedRegs:")
    print(f"  rax={u64(savedRegs.rax)} rbx={u64(savedRegs.rbx)} rcx={u64(savedRegs.rcx)} rdx={u64(savedRegs.rdx)}")

    print("\nTask:")
    print(f"  entry={u64(taskEntry)} pagemap={u64(taskPagemap)}")
    print(f"  elfHeaderAddr={u64(elfHeaderAddr)} programHeaderAddr={u64(programHeaderAddr)}")

    print("\nSegments:")
    # Segment table is written as a fixed-size array of 5 segments (MAX_STACK_PAGES=4 + fault page)
    # but header reports segmentCount.
    seg_table_off = segmentTableOffset
    # Read 5 entries to be safe, but only print segmentCount.
    segs = []
    for i in range(5):
        vaddr, size, fileOffset, stype, present = struct.unpack_from("<QQQII", data, seg_table_off + i * (8 + 8 + 8 + 4 + 4))
        segs.append((vaddr, size, fileOffset, stype, present))

    for i in range(int(segmentCount)):
        vaddr, size, fileOffset, stype, present = segs[i]
        print(f"  [{i}] type={stype} present={present} vaddr={u64(vaddr)} size={size} fileOffset={fileOffset}")

    print("\nELF:")
    print(f"  elfSize={elfSize} elfOffset={elfOffset}")
    if elfSize:
        elf_magic = data[elfOffset : elfOffset + 4]
        print(f"  elfMagic={elf_magic!r}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
