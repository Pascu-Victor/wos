#!/usr/bin/env python3

import argparse
import bisect
import struct
import subprocess
import sys
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Optional


COREDUMP_MAGIC = 0x504D55444F43534F  # "WOSCODMP" little-endian-ish

SEGMENT_TYPES = {
    0: "Zero/Unmapped",
    1: "StackPage",
    2: "FaultPage",
}

SEGMENT_SIZE = 8 + 8 + 8 + 4 + 4  # 28 bytes per CoreDumpSegment
MAX_SEGMENTS = 5  # MAX_STACK_PAGES (4) + 1 fault page


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


@dataclass
class CoreDumpSegment:
    vaddr: int
    size: int
    fileOffset: int
    type: int
    present: int

    @property
    def type_name(self) -> str:
        return SEGMENT_TYPES.get(self.type, f"Unknown({self.type})")

    @property
    def vaddr_end(self) -> int:
        return self.vaddr + self.size


@dataclass
class CoreDump:
    """Parsed WOS core dump."""
    magic: int
    version: int
    headerSize: int
    timestamp: int
    pid: int
    cpu: int
    intNum: int
    errCode: int
    cr2: int
    cr3: int
    trapFrame: InterruptFrame
    trapRegs: GPRegs
    savedFrame: InterruptFrame
    savedRegs: GPRegs
    taskEntry: int
    taskPagemap: int
    elfHeaderAddr: int
    programHeaderAddr: int
    segmentCount: int
    segmentTableOffset: int
    elfSize: int
    elfOffset: int
    segments: list[CoreDumpSegment]
    raw: bytes  # full file contents


# --- ELF64 symbol table parsing ---

ELF_MAGIC = b"\x7fELF"
SHT_SYMTAB = 2
SHT_DYNSYM = 11
SHF_ALLOC = 0x2
STT_FUNC = 2
STT_NOTYPE = 0


def _demangle_batch(names: list[str]) -> list[str]:
    """Demangle a batch of C++ symbol names via llvm-cxxfilt."""
    if not names:
        return names
    try:
        proc = subprocess.run(
            ["llvm-cxxfilt"],
            input="\n".join(names),
            capture_output=True, text=True, timeout=10,
        )
        if proc.returncode == 0:
            result = proc.stdout.rstrip("\n").split("\n")
            if len(result) == len(names):
                return result
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return names


class SymbolTable:
    """Sorted symbol table for address-to-name lookups."""

    def __init__(self) -> None:
        # Sorted by address; each entry is (addr, name, size).
        self._syms: list[tuple[int, str, int]] = []

    @property
    def count(self) -> int:
        return len(self._syms)

    def add(self, addr: int, name: str, size: int) -> None:
        self._syms.append((addr, name, size))

    def finish(self) -> None:
        """Sort the table and demangle C++ names."""
        demangled = _demangle_batch([name for _, name, _ in self._syms])
        self._syms = [
            (addr, dm, size)
            for (addr, _, size), dm in zip(self._syms, demangled)
        ]
        self._syms.sort()

    def lookup(self, addr: int) -> Optional[str]:
        """Find the symbol containing or nearest-below `addr`.

        Returns a string like "func_name+0x1a" or None if no plausible match.
        """
        if not self._syms:
            return None
        idx = bisect.bisect_right(self._syms, (addr,)) - 1
        if idx < 0:
            return None
        sym_addr, sym_name, sym_size = self._syms[idx]
        offset = addr - sym_addr
        if offset < 0:
            return None
        # If the symbol has a known size, only match within it.
        # If size is 0 (unknown), allow a reasonable offset (e.g. 0x10000).
        if sym_size > 0 and offset >= sym_size:
            # Fall through — still report if offset is small, as sizes
            # can be inaccurate in hand-written asm.
            if offset > 0x1000:
                return None
        elif sym_size == 0 and offset > 0x10000:
            return None
        if offset == 0:
            return sym_name
        return f"{sym_name}+0x{offset:x}"


class SectionMap:
    """Maps virtual addresses to ELF section names."""

    def __init__(self) -> None:
        # Sorted by address; each entry is (vaddr, size, name).
        self._sections: list[tuple[int, int, str]] = []

    @property
    def count(self) -> int:
        return len(self._sections)

    def add(self, vaddr: int, size: int, name: str) -> None:
        self._sections.append((vaddr, size, name))

    def finish(self) -> None:
        self._sections.sort()

    def lookup(self, addr: int) -> Optional[str]:
        """Find the section containing `addr`.

        Returns a string like ".text+0x1a" or None.
        """
        if not self._sections:
            return None
        idx = bisect.bisect_right(self._sections, (addr,)) - 1
        if idx < 0:
            return None
        sec_addr, sec_size, sec_name = self._sections[idx]
        offset = addr - sec_addr
        if offset < 0 or offset >= sec_size:
            return None
        if offset == 0:
            return sec_name
        return f"{sec_name}+0x{offset:x}"


def _parse_elf_sections(elf: bytes) -> Optional[SectionMap]:
    """Parse allocated section headers from a raw ELF64 image."""
    if len(elf) < 64 or elf[:4] != ELF_MAGIC:
        return None
    if elf[4] != 2:  # ELF64
        return None

    (e_shoff,) = struct.unpack_from("<Q", elf, 40)
    (e_shentsize, e_shnum, e_shstrndx) = struct.unpack_from("<HHH", elf, 58)

    if e_shoff == 0 or e_shnum == 0 or e_shstrndx >= e_shnum:
        return None

    def _read_shdr(i: int) -> Optional[tuple]:
        off = e_shoff + i * e_shentsize
        if off + 64 > len(elf):
            return None
        return struct.unpack_from("<IIQQQQIIqq", elf, off)

    # Load section name string table.
    shstr = _read_shdr(e_shstrndx)
    if shstr is None:
        return None
    shstrtab_off = shstr[4]   # sh_offset
    shstrtab_size = shstr[5]  # sh_size
    shstrtab = elf[shstrtab_off:shstrtab_off + shstrtab_size]

    def _section_name(name_off: int) -> str:
        end = shstrtab.find(b"\x00", name_off)
        if end < 0:
            end = len(shstrtab)
        return shstrtab[name_off:end].decode("utf-8", errors="replace")

    smap = SectionMap()
    for i in range(e_shnum):
        hdr = _read_shdr(i)
        if hdr is None:
            continue
        sh_name, sh_type, sh_flags, sh_addr, sh_offset, sh_size = hdr[:6]
        if not (sh_flags & SHF_ALLOC):
            continue
        if sh_addr == 0 or sh_size == 0:
            continue
        name = _section_name(sh_name)
        if name:
            smap.add(sh_addr, sh_size, name)

    smap.finish()
    return smap if smap.count > 0 else None


def _parse_elf_symtab(elf: bytes) -> Optional[SymbolTable]:
    """Parse .symtab (or .dynsym) from a raw ELF64 image."""
    if len(elf) < 64 or elf[:4] != ELF_MAGIC:
        return None
    ei_class = elf[4]
    if ei_class != 2:  # Must be ELF64
        return None

    (e_shoff,) = struct.unpack_from("<Q", elf, 40)
    (e_shentsize, e_shnum, e_shstrndx) = struct.unpack_from("<HHH", elf, 58)

    if e_shoff == 0 or e_shnum == 0:
        return None

    # Read section headers.
    shdrs = []
    for i in range(e_shnum):
        off = e_shoff + i * e_shentsize
        if off + 64 > len(elf):
            return None
        (sh_name, sh_type, sh_flags, sh_addr, sh_offset, sh_size,
         sh_link, sh_info, sh_addralign, sh_entsize) = struct.unpack_from(
            "<IIQQQQIIqq", elf, off,
        )
        shdrs.append({
            "name_off": sh_name, "type": sh_type, "offset": sh_offset,
            "size": sh_size, "link": sh_link, "entsize": sh_entsize,
        })

    # Find symtab (prefer .symtab over .dynsym).
    symtab_shdr = None
    for stype in (SHT_SYMTAB, SHT_DYNSYM):
        for s in shdrs:
            if s["type"] == stype:
                symtab_shdr = s
                break
        if symtab_shdr is not None:
            break
    if symtab_shdr is None:
        return None

    # The linked section is the string table for symbol names.
    strtab_idx = symtab_shdr["link"]
    if strtab_idx >= len(shdrs):
        return None
    strtab_shdr = shdrs[strtab_idx]
    strtab = elf[strtab_shdr["offset"]:strtab_shdr["offset"] + strtab_shdr["size"]]

    # Parse symbol entries.
    entsize = symtab_shdr["entsize"] or 24  # Elf64_Sym is 24 bytes
    sym_off = symtab_shdr["offset"]
    sym_end = sym_off + symtab_shdr["size"]
    table = SymbolTable()

    while sym_off + entsize <= sym_end:
        (st_name, st_info, st_other, st_shndx,
         st_value, st_size) = struct.unpack_from("<IBBHQQ", elf, sym_off)
        sym_off += entsize

        stt = st_info & 0xF
        if stt not in (STT_FUNC, STT_NOTYPE):
            continue
        if st_value == 0:
            continue

        # Read null-terminated name from strtab.
        name_end = strtab.find(b"\x00", st_name)
        if name_end < 0:
            name_end = len(strtab)
        name = strtab[st_name:name_end].decode("utf-8", errors="replace")
        if not name:
            continue
        table.add(st_value, name, st_size)

    table.finish()
    return table if table.count > 0 else None


def _get_elf_bytes(path: Optional[Path] = None, dump: Optional["CoreDump"] = None) -> Optional[bytes]:
    """Return raw ELF bytes from a file path or embedded in a coredump."""
    if path is not None:
        try:
            return path.read_bytes()
        except OSError as e:
            print(f"Warning: could not read {path}: {e}", file=sys.stderr)
            return None
    if dump is not None and dump.elfSize > 0 and dump.elfOffset > 0:
        return dump.raw[dump.elfOffset:dump.elfOffset + dump.elfSize]
    return None


def load_symbols_from_elf(path: Path) -> Optional[SymbolTable]:
    """Load a symbol table from an external ELF file."""
    elf = _get_elf_bytes(path=path)
    return _parse_elf_symtab(elf) if elf else None


def load_symbols_from_coredump(dump: "CoreDump") -> Optional[SymbolTable]:
    """Extract and parse the embedded ELF's symbol table from a coredump."""
    elf = _get_elf_bytes(dump=dump)
    return _parse_elf_symtab(elf) if elf else None


def load_sections_from_elf(path: Path) -> Optional[SectionMap]:
    """Load a section map from an external ELF file."""
    elf = _get_elf_bytes(path=path)
    return _parse_elf_sections(elf) if elf else None


def load_sections_from_coredump(dump: "CoreDump") -> Optional[SectionMap]:
    """Extract and parse the embedded ELF's section headers from a coredump."""
    elf = _get_elf_bytes(dump=dump)
    return _parse_elf_sections(elf) if elf else None


def resolve_addr(addr: int, tables: list[SymbolTable],
                 section_maps: Optional[list[SectionMap]] = None) -> Optional[str]:
    """Try to resolve an address: symbols first, then section fallback."""
    for t in tables:
        result = t.lookup(addr)
        if result is not None:
            return result
    if section_maps:
        for m in section_maps:
            result = m.lookup(addr)
            if result is not None:
                return result
    return None


def u64(x: int) -> str:
    return f"0x{x:016x}"


def parse_int(s: str) -> int:
    """Parse an integer from a string, supporting 0x hex prefix."""
    s = s.strip()
    if s.startswith("0x") or s.startswith("0X"):
        return int(s, 16)
    return int(s)


def parse_interrupt_frame(buf: bytes, off: int) -> tuple[InterruptFrame, int]:
    vals = struct.unpack_from("<7Q", buf, off)
    return InterruptFrame(*vals), off + 7 * 8


def parse_gpregs(buf: bytes, off: int) -> tuple[GPRegs, int]:
    vals = struct.unpack_from("<15Q", buf, off)
    return GPRegs(*vals), off + 15 * 8


def parse_coredump(data: bytes) -> CoreDump:
    off = 0
    (magic, version, headerSize) = struct.unpack_from("<QII", data, off)
    off += 8 + 4 + 4

    if magic != COREDUMP_MAGIC:
        raise SystemExit(f"Bad magic: {u64(magic)} (expected {u64(COREDUMP_MAGIC)})")

    (timestamp, pid, cpu, intNum, errCode, cr2, cr3) = struct.unpack_from("<7Q", data, off)
    off += 7 * 8

    trapFrame, off = parse_interrupt_frame(data, off)
    trapRegs, off = parse_gpregs(data, off)
    savedFrame, off = parse_interrupt_frame(data, off)
    savedRegs, off = parse_gpregs(data, off)

    (taskEntry, taskPagemap, elfHeaderAddr, programHeaderAddr,
     segmentCount, segmentTableOffset, elfSize, elfOffset) = struct.unpack_from("<8Q", data, off)
    off += 8 * 8

    # Parse segment table (fixed array of MAX_SEGMENTS entries).
    segments = []
    for i in range(MAX_SEGMENTS):
        soff = segmentTableOffset + i * SEGMENT_SIZE
        vaddr, size, fileOffset, stype, present = struct.unpack_from("<QQQII", data, soff)
        segments.append(CoreDumpSegment(vaddr, size, fileOffset, stype, present))

    return CoreDump(
        magic=magic, version=version, headerSize=headerSize,
        timestamp=timestamp, pid=pid, cpu=cpu,
        intNum=intNum, errCode=errCode, cr2=cr2, cr3=cr3,
        trapFrame=trapFrame, trapRegs=trapRegs,
        savedFrame=savedFrame, savedRegs=savedRegs,
        taskEntry=taskEntry, taskPagemap=taskPagemap,
        elfHeaderAddr=elfHeaderAddr, programHeaderAddr=programHeaderAddr,
        segmentCount=segmentCount, segmentTableOffset=segmentTableOffset,
        elfSize=elfSize, elfOffset=elfOffset,
        segments=segments, raw=data,
    )


def _fmt_addr(addr: int, sym_tables: Optional[list[SymbolTable]] = None,
              section_maps: Optional[list[SectionMap]] = None) -> str:
    """Format an address with optional symbol/section resolution."""
    base = u64(addr)
    if sym_tables or section_maps:
        sym = resolve_addr(addr, sym_tables or [], section_maps)
        if sym:
            return f"{base} <{sym}>"
    return base


def print_header(dump: CoreDump, path: Path,
                 sym_tables: Optional[list[SymbolTable]] = None,
                 section_maps: Optional[list[SectionMap]] = None) -> None:
    fa = lambda addr: _fmt_addr(addr, sym_tables, section_maps)
    print(f"file: {path}")
    print(f"magic: {u64(dump.magic)} version: {dump.version} headerSize: {dump.headerSize}")
    print(f"timestampQuantums: {dump.timestamp}")
    print(f"pid: {dump.pid} cpu: {dump.cpu}")
    print(f"intNum: {dump.intNum} ({interrupt_name(dump.intNum)}) errCode: {u64(dump.errCode)}")
    print(f"cr2: {fa(dump.cr2)} cr3: {u64(dump.cr3)}")

    print("\ntrapFrame:")
    tf = dump.trapFrame
    print(f"  rip={fa(tf.rip)} cs={u64(tf.cs)} rflags={u64(tf.rflags)} rsp={u64(tf.rsp)} ss={u64(tf.ss)}")
    print("trapRegs:")
    tr = dump.trapRegs
    print(f"  rax={u64(tr.rax)} rbx={u64(tr.rbx)} rcx={u64(tr.rcx)} rdx={u64(tr.rdx)}")
    print(f"  rsi={u64(tr.rsi)} rdi={u64(tr.rdi)} rbp={u64(tr.rbp)}")
    print(f"  r8={u64(tr.r8)}  r9={u64(tr.r9)}  r10={u64(tr.r10)} r11={u64(tr.r11)}")
    print(f"  r12={u64(tr.r12)} r13={u64(tr.r13)} r14={u64(tr.r14)} r15={u64(tr.r15)}")

    print("\nsavedFrame:")
    sf = dump.savedFrame
    print(f"  rip={fa(sf.rip)} cs={u64(sf.cs)} rflags={u64(sf.rflags)} rsp={u64(sf.rsp)} ss={u64(sf.ss)}")
    print("savedRegs:")
    sr = dump.savedRegs
    print(f"  rax={u64(sr.rax)} rbx={u64(sr.rbx)} rcx={u64(sr.rcx)} rdx={u64(sr.rdx)}")
    print(f"  rsi={u64(sr.rsi)} rdi={u64(sr.rdi)} rbp={u64(sr.rbp)}")
    print(f"  r8={u64(sr.r8)}  r9={u64(sr.r9)}  r10={u64(sr.r10)} r11={u64(sr.r11)}")
    print(f"  r12={u64(sr.r12)} r13={u64(sr.r13)} r14={u64(sr.r14)} r15={u64(sr.r15)}")

    print("\nTask:")
    print(f"  entry={fa(dump.taskEntry)} pagemap={u64(dump.taskPagemap)}")
    print(f"  elfHeaderAddr={u64(dump.elfHeaderAddr)} programHeaderAddr={u64(dump.programHeaderAddr)}")


def print_segments(dump: CoreDump) -> None:
    print("\nSegments:")
    for i in range(int(dump.segmentCount)):
        seg = dump.segments[i]
        present_str = "present" if seg.present else "NOT present"
        print(f"  [{i}] {seg.type_name:12s}  vaddr={u64(seg.vaddr)}..{u64(seg.vaddr_end)}  "
              f"size=0x{seg.size:x}  fileOffset=0x{seg.fileOffset:x}  {present_str}")


def print_elf(dump: CoreDump) -> None:
    print("\nELF:")
    print(f"  elfSize={dump.elfSize} elfOffset={dump.elfOffset}")
    if dump.elfSize and dump.elfOffset < len(dump.raw):
        elf_magic = dump.raw[dump.elfOffset:dump.elfOffset + 4]
        print(f"  elfMagic={elf_magic!r}")


def interrupt_name(num: int) -> str:
    names = {
        0: "#DE Divide Error",
        1: "#DB Debug",
        2: "NMI",
        3: "#BP Breakpoint",
        4: "#OF Overflow",
        5: "#BR Bound Range",
        6: "#UD Invalid Opcode",
        7: "#NM Device Not Available",
        8: "#DF Double Fault",
        13: "#GP General Protection",
        14: "#PF Page Fault",
        16: "#MF x87 FP",
        17: "#AC Alignment Check",
        18: "#MC Machine Check",
        19: "#XM SIMD FP",
    }
    return names.get(num, f"INT {num}")


def find_segment_for_va(dump: CoreDump, va: int) -> Optional[CoreDumpSegment]:
    """Find the segment that contains a given virtual address."""
    for seg in dump.segments[:int(dump.segmentCount)]:
        if seg.present and seg.vaddr <= va < seg.vaddr_end:
            return seg
    return None


def read_va_bytes(dump: CoreDump, va_start: int, length: int) -> Optional[bytes]:
    """Read `length` bytes starting at virtual address `va_start` from coredump segments.

    Returns None if the address range is not fully covered by present segments.
    For ranges spanning multiple pages, this stitches together data from multiple segments.
    """
    result = bytearray()
    va = va_start
    remaining = length
    while remaining > 0:
        seg = find_segment_for_va(dump, va)
        if seg is None:
            return None
        # How many bytes can we read from this segment?
        seg_offset = va - seg.vaddr
        avail = seg.size - seg_offset
        to_read = min(avail, remaining)
        file_off = seg.fileOffset + seg_offset
        result.extend(dump.raw[file_off:file_off + to_read])
        va += to_read
        remaining -= to_read
    return bytes(result)


def annotate_qword(va: int, value: int, dump: CoreDump,
                    sym_tables: Optional[list[SymbolTable]] = None,
                    section_maps: Optional[list[SectionMap]] = None) -> str:
    """Generate annotation hints for a qword value at a given virtual address."""
    notes = []
    trap_rsp = dump.trapFrame.rsp
    trap_rbp = dump.trapRegs.rbp
    saved_rsp = dump.savedFrame.rsp
    saved_rbp = dump.savedRegs.rbp

    # RSP/RBP markers
    if va == trap_rsp:
        notes.append("<-- trap RSP")
    if va == trap_rsp - 8:
        notes.append("<-- trap RSP-8")
    if va == trap_rsp + 8:
        notes.append("<-- trap RSP+8")
    if va == trap_rbp:
        notes.append("<-- trap RBP")
    if va == saved_rsp:
        notes.append("<-- saved RSP")
    if va == saved_rbp:
        notes.append("<-- saved RBP")

    # Value heuristics
    if value == 0:
        notes.append("[zero]")
    elif 0 < value < 0x1000:
        notes.append(f"[small: {value}]")
    elif 0x400000 <= value <= 0xFFFFFF:
        sym = resolve_addr(value, sym_tables or [], section_maps)
        notes.append(f"[code: {sym}]" if sym else "[code addr?]")
    elif (value >> 40) == 0x7ffe or (value >> 40) == 0x7fff:
        notes.append("[stack ptr?]")
    elif value == dump.trapFrame.rip:
        notes.append("[== trap RIP]")
    elif value == dump.savedFrame.rip:
        notes.append("[== saved RIP]")

    return "  ".join(notes)


def cmd_dump_range(dump: CoreDump, va_start: int, va_end: int,
                    sym_tables: Optional[list[SymbolTable]] = None,
                    section_maps: Optional[list[SectionMap]] = None) -> int:
    """Dump memory from va_start to va_end as annotated qwords + raw hex."""
    # Align start down to 8-byte boundary.
    va_start_aligned = va_start & ~7
    length = va_end - va_start_aligned

    if length <= 0:
        print("Error: end address must be greater than start address.", file=sys.stderr)
        return 1
    if length > 0x10000:
        print(f"Error: requested range too large ({length} bytes, max 64KiB).", file=sys.stderr)
        return 1

    data = read_va_bytes(dump, va_start_aligned, length)
    if data is None:
        # Try to identify which parts are missing.
        print(f"Error: address range {u64(va_start_aligned)}..{u64(va_end)} is not fully covered by present segments.", file=sys.stderr)
        print("Available segments:", file=sys.stderr)
        for i, seg in enumerate(dump.segments[:int(dump.segmentCount)]):
            present_str = "present" if seg.present else "NOT present"
            print(f"  [{i}] {u64(seg.vaddr)}..{u64(seg.vaddr_end)} {present_str}", file=sys.stderr)
        return 1

    trap_rsp = dump.trapFrame.rsp

    # --- Qword dump ---
    # x86-64: stack grows downward (toward lower addresses).
    print(f"\n{'=' * 95}")
    print(f"  Memory dump: {u64(va_start_aligned)} .. {u64(va_end)}  ({length} bytes, {length // 8} qwords)")
    print(f"  Trap RSP: {u64(trap_rsp)}  Trap RIP: {u64(dump.trapFrame.rip)}")
    print(f"  Stack grows toward lower addresses (v); callers toward higher addresses (^)")
    print(f"{'=' * 95}")
    print(f"     {'VIRTUAL ADDRESS':<22s}  {'VALUE (LE uint64)':<20s}  NOTES")
    print(f"     {'-' * 20}  {'-' * 18}  {'-' * 40}")

    for off in range(0, len(data) - 7, 8):
        va = va_start_aligned + off
        qword = struct.unpack_from("<Q", data, off)[0]
        notes = annotate_qword(va, qword, dump, sym_tables, section_maps)
        if va < trap_rsp:
            gutter = " v "  # below RSP — stack growth direction
        elif va == trap_rsp:
            gutter = ">>>"  # current stack pointer
        else:
            gutter = " ^ "  # above RSP — toward caller frames
        print(f"  {gutter} {u64(va)}    {u64(qword)}  {notes}")

    print(f"{'=' * 95}")

    # --- Raw hex dump ---
    print(f"\n  Raw hex bytes:")
    for off in range(0, len(data), 16):
        va = va_start_aligned + off
        chunk = data[off:off + 16]
        hexbytes = " ".join(f"{b:02x}" for b in chunk)
        ascii_repr = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
        print(f"  {u64(va)}:  {hexbytes:<48s}  |{ascii_repr}|")

    print()
    return 0


def cmd_dump_segment(dump: CoreDump, seg_index: int,
                     sym_tables: Optional[list[SymbolTable]] = None,
                     section_maps: Optional[list[SectionMap]] = None) -> int:
    """Hex-dump the full contents of a segment by index."""
    if seg_index < 0 or seg_index >= int(dump.segmentCount):
        print(f"Error: segment index {seg_index} out of range (0..{int(dump.segmentCount) - 1}).", file=sys.stderr)
        return 1
    seg = dump.segments[seg_index]
    if not seg.present:
        print(f"Error: segment [{seg_index}] is not present in the dump.", file=sys.stderr)
        return 1
    return cmd_dump_range(dump, seg.vaddr, seg.vaddr_end, sym_tables, section_maps)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Parse and analyze WOS core dump binaries.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  %(prog)s coredump.bin
      Print header, registers, segments, and ELF info.

  %(prog)s coredump.bin --dump-range 0x7ffefffeed98 0x7ffefffeef00
      Dump memory in the given virtual address range as annotated qwords.

  %(prog)s coredump.bin --dump-segment 0
      Dump the full contents of segment 0.

  %(prog)s coredump.bin --symbols kernel.elf --symbols app.elf
      Resolve code addresses using symbols from external ELF files.
      Symbols from the embedded ELF are always loaded automatically.
""",
    )
    ap.add_argument("file", type=Path, help="Path to the .bin coredump file")
    ap.add_argument(
        "--dump-range", nargs=2, metavar=("VA_START", "VA_END"),
        help="Dump memory from VA_START to VA_END (hex or decimal) as annotated qwords + raw hex",
    )
    ap.add_argument(
        "--dump-segment", type=int, metavar="INDEX",
        help="Dump the full contents of the segment at the given index",
    )
    ap.add_argument(
        "--symbols", type=Path, metavar="ELF", action="append", default=[],
        help="Load symbols from an external ELF file (can be repeated)",
    )
    args = ap.parse_args()

    data = args.file.read_bytes()
    dump = parse_coredump(data)

    # Build symbol tables and section maps: embedded ELF first, then externals.
    sym_tables: list[SymbolTable] = []
    section_maps: list[SectionMap] = []

    embedded_syms = load_symbols_from_coredump(dump)
    if embedded_syms is not None:
        print(f"Loaded {embedded_syms.count} symbols from embedded ELF")
        sym_tables.append(embedded_syms)
    embedded_secs = load_sections_from_coredump(dump)
    if embedded_secs is not None:
        print(f"Loaded {embedded_secs.count} sections from embedded ELF")
        section_maps.append(embedded_secs)

    for sym_path in args.symbols:
        ext_syms = load_symbols_from_elf(sym_path)
        if ext_syms is not None:
            print(f"Loaded {ext_syms.count} symbols from {sym_path}")
            sym_tables.append(ext_syms)
        ext_secs = load_sections_from_elf(sym_path)
        if ext_secs is not None:
            print(f"Loaded {ext_secs.count} sections from {sym_path}")
            section_maps.append(ext_secs)
        if ext_syms is None and ext_secs is None:
            print(f"Warning: no symbols or sections found in {sym_path}", file=sys.stderr)

    st = sym_tables or None
    sm = section_maps or None

    # Always print header info.
    print_header(dump, args.file, st, sm)
    print_segments(dump)
    print_elf(dump)

    # Handle subcommands.
    if args.dump_range is not None:
        va_start = parse_int(args.dump_range[0])
        va_end = parse_int(args.dump_range[1])
        return cmd_dump_range(dump, va_start, va_end, st, sm)

    if args.dump_segment is not None:
        return cmd_dump_segment(dump, args.dump_segment, st, sm)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
