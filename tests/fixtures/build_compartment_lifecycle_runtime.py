#!/usr/bin/env python3
"""Build four fresh lifecycle fixture ELFs and an actual manifest; no VM actions.

All compilation/inspection helpers are local and stdlib-only. Runtime never
reads the historical pilot/untracked builders or any prior evidence receipt.
"""
import argparse,ctypes,hashlib,json,os,re,resource,secrets,shlex,shutil,signal,struct,subprocess,sys,tempfile,time
from pathlib import Path
sys.dont_write_bytecode=True
R=O=None
WHOLE=120
OUTPUT_BUDGET=32<<20
SCRATCH_BUDGET=64<<20
LOG_BUDGET=1<<20
FILE_BUDGET=8<<20
MAX_ARTIFACT=65536
MAX_COMMAND_FILE_BYTES=1<<20
INTERPRETER='/lib/lifecycle-tiny-ld.so'
def digest(data):return hashlib.sha256(data).hexdigest()

def require(value,why):
 if not value:raise ValueError(why)

def pin(p):
 p=Path(p).absolute();st=p.stat();h=hashlib.sha256()
 with p.open('rb')as f:
  for chunk in iter(lambda:f.read(1<<20),b''):h.update(chunk)
 return {'path':str(p),'resolved':str(p.resolve(strict=True)),'bytes':st.st_size,'sha256':h.hexdigest(),'symlink':p.is_symlink(),'link':os.readlink(p)if p.is_symlink()else None}

def depfiles(path):
 require(path.stat().st_size<=1<<20,'dependency file >1MiB')
 body=path.read_text().replace('\\\n',' ').splitlines()[0];require(': 'in body,'dependency missing target separator')
 return {Path(os.path.normpath(str(R/p)))for p in shlex.split(body.split(': ',1)[1])}

def output_usage():
 files=[p for p in O.rglob('*')if p.is_file()];require(all(not p.is_symlink()for p in O.rglob('*')),'unexpected output symlink')
 logical=sum(p.stat().st_size for p in files);allocated=sum(p.stat().st_blocks*512 for p in files)
 require(logical<=OUTPUT_BUDGET and len(files)<=96,'bounded output directory exceeded')
 return {'files':len(files),'logical_bytes':logical,'allocated_bytes':allocated}

def save(p,d):
 with p.open('x')as f:json.dump(d,f,indent=2,sort_keys=True);f.write('\n')

def contained(start, size, regions):
    return any(start >= begin and size <= end - start for begin, end in regions if start <= end)

def elf_gate(path, role):
    data = path.read_bytes()
    require(64 <= len(data) <= MAX_ARTIFACT, f"ELF size outside genuine fallback bound: {path}")
    header = struct.unpack_from("<16sHHIQQQIHHHHHH", data)
    ident, kind, machine, version, entry, phoff, shoff, flags, ehsize, phentsize, phnum, shentsize, shnum, shstr = header
    require(ident[:7] == b"\x7fELF\x02\x01\x01" and kind == (3 if role == "interpreter" else 2) and machine == 62 and version == 1,
            f"wrong ELF identity/role: {path}")
    require(ehsize == 64 and phentsize == 56 and 1 <= phnum <= 32 and phoff >= 64 and phoff + phnum * 56 <= len(data), "invalid PHDR table")
    require(shentsize == 64 and 1 <= shnum <= 512 and 0 < shstr < shnum and shoff >= 64 and shoff + shnum * 64 <= len(data), "invalid section table")
    programs = [struct.unpack_from("<IIQQQQQQ", data, phoff + i * 56) for i in range(phnum)]
    loads = []
    readable_file_ranges = []
    executable_ranges = []
    interpreter = []
    stack = []
    dynamic = []
    for index, p in enumerate(programs):
        ptype, pflags, offset, vaddr, paddr, filesz, memsz, alignment = p
        require(offset <= len(data) and filesz <= len(data) - offset and filesz <= memsz, f"invalid program range {index}")
        require(vaddr < (1 << 47) and memsz <= (1 << 47) - vaddr, f"invalid program address {index}")
        if ptype == 1:
            require(pflags & 4 and not (pflags & 3 == 3) and memsz > 0, "unreadable/RWX/empty load")
            require(alignment in (0, 1) or (alignment <= 2 * 1024 * 1024 and alignment & (alignment - 1) == 0), "invalid load alignment")
            require((offset & 4095) == (vaddr & 4095) and (alignment <= 1 or offset % alignment == vaddr % alignment), "incongruent load")
            begin, end = vaddr & ~4095, (vaddr + memsz + 4095) & ~4095
            require(not any(begin < old_end and old_begin < end for old_begin, old_end in loads), "overlapping load pages")
            loads.append((begin, end))
            if not pflags & 1:
                readable_file_ranges.append((vaddr, vaddr + filesz))
            if pflags & 1:
                executable_ranges.append((vaddr, vaddr + filesz))
            if role != "interpreter":
                require(0x400000 <= begin and end <= 0x800000, "main outside fixed image window")
        elif ptype == 3:
            require(1 <= filesz <= 512 and data[offset + filesz - 1] == 0 and b"\0" not in data[offset:offset + filesz - 1], "malformed PT_INTERP")
            interpreter.append(data[offset:offset + filesz - 1].decode("ascii"))
        elif ptype == 2:
            dynamic.append((offset, filesz, vaddr))
        elif ptype == 7:
            raise RuntimeError("TLS is not admitted")
        elif ptype == 0x6474E551:
            require(not pflags & 1, "executable GNU_STACK")
            stack.append(p)
    require(loads and contained(entry, 1, executable_ranges), "entry is not file-backed executable code")
    require(len(stack) == 1, "missing/duplicate explicit NX GNU_STACK")
    if role == "interpreter":
        require(not interpreter and 0 < entry <= MAX_ARTIFACT, "interpreter path/entry invalid")
        require(any(p[0] == 1 and p[2] == 0 and p[3] == 0 and p[1] == 4 and p[5] >= phoff + phnum * 56 for p in programs), "no readable vaddr0 ELF/PHDR header load")
    else:
        require(interpreter == ([INTERPRETER] if role == "tiny-main" else []), "wrong main interpreter shape")
        require(phoff == 64 and any(p[0] == 1 and p[2] == 0 and p[3] == 0x400000 and p[1] == 4 and
                                   p[5] >= 64 + phnum * 56 for p in programs), "main does not contain the exact real header bytes")
    # Loader metadata must be contained in nonexecuting readable file bytes.
    for p in programs:
        if p[0] in (2, 3, 6, 0x6474E550, 0x6474E552) and p[5]:
            require(contained(p[3], p[5], readable_file_ranges), "metadata outside readable nonexecuting load")
    sections = [struct.unpack_from("<IIQQQQIIQQ", data, shoff + i * 64) for i in range(shnum)]
    symbols = []
    for s in sections:
        name, stype, sflags, saddr, offset, size, link, info, alignment, entsize = s
        require(stype == 8 or (offset <= len(data) and size <= len(data) - offset), "section bytes outside ELF")
        require(not (stype in (4, 9, 19, 14, 15, 16) and size), "relocations or constructors are not admitted")
        if stype in (2, 11):
            require(entsize == 24 and size % 24 == 0 and link < shnum, "invalid symbol table")
            strings = sections[link]
            require(strings[1] == 3 and strings[4] <= len(data) and strings[5] <= len(data) - strings[4], "invalid symbol string table")
            string_data = data[strings[4]:strings[4] + strings[5]]
            for pos in range(offset, offset + size, 24):
                sn, si, so, ss, sv, sz = struct.unpack_from("<IBBHQQ", data, pos)
                require(sn < len(string_data), "symbol name outside string table")
                nul = string_data.find(b"\0", sn)
                require(nul >= sn, "unterminated symbol name")
                text = string_data[sn:nul].decode("ascii")
                require(not (ss == 0 and si >> 4 != 0), f"undefined nonlocal symbol: {text}")
                require(si & 15 not in (6, 10), "TLS/IFUNC symbol")
                symbols.append({"name": text, "value": sv, "size": sz, "binding": si >> 4, "type": si & 15, "visibility": so & 3, "section": ss})
    starts = [s for s in symbols if s["name"] == "_start" and s["section"] != 0]
    require(starts and all(s["value"] == entry and s["visibility"] == 2 and s["type"] == 2 for s in starts), "hidden resolved _start mismatch")
    forbidden_tags = {1, 2, 7, 8, 9, 12, 13, 17, 18, 19, 23, 25, 26, 27, 28, 32, 33, 35, 36, 37}
    dynamic_rows = []
    require(len(dynamic) <= 1, "duplicate dynamic segment")
    for offset, size, vaddr in dynamic:
        require(size > 0 and size % 16 == 0, "invalid dynamic table")
        terminated = False
        for pos in range(offset, offset + size, 16):
            tag, value = struct.unpack_from("<qQ", data, pos)
            if tag == 0:
                terminated = True
                require(all(byte == 0 for byte in data[pos:offset + size]), "nonzero data after DT_NULL")
                break
            require(tag not in forbidden_tags, f"runtime dependency/relocation/initializer tag: {tag}")
            if tag in (4, 5, 6, 0x6FFFFEF5):
                require(contained(value, 1, readable_file_ranges), "dynamic metadata pointer outside readable load")
            dynamic_rows.append({"tag": tag, "value": value})
        require(terminated, "unterminated dynamic table")
    return {"path": str(path), "bytes": len(data), "sha256": digest(data), "role": role, "elf_type": kind, "entry": entry,
            "phoff": phoff, "phnum": phnum, "phentsize": phentsize, "expected_native_at_phdr": 0x1040 if role != "interpreter" else None,
            "loads": loads, "programs": programs, "interpreter": interpreter, "dynamic": dynamic_rows,
            "symbols": symbols, "structured_elf_gate": "PASS", "private_startup_qualified": False}

def instructions(path):
    result = []
    for line in path.read_text().splitlines():
        match = re.match(r"^\s*([0-9a-fA-F]+):\s+([a-zA-Z][a-zA-Z0-9.]*)\s*(.*)$", line)
        if match:
            result.append((int(match[1], 16), match[2], match[3].split("#", 1)[0].strip()))
    require(result, f"no parsed instructions: {path}")
    return result

def entry_disassembly_gate(path, elf, role):
    parsed = instructions(path)
    starts = [s for s in elf["symbols"] if s["name"] == "_start"]
    size = next(s["size"] for s in starts if s["size"] > 0)
    entry = elf["entry"]
    body = [row for row in parsed if entry <= row[0] < entry + size]
    require(body and body[0][0] == entry, "disassembly misses actual entry")
    if role == "interpreter":
        transfers = [i for i, row in enumerate(body) if row[1].startswith("jmp") and row[2] == "*%r8"]
        require(len(transfers) == 1, "missing/duplicate exact AT_ENTRY register jump")
        prefix = body[:transfers[0] + 1]
        for _, mnemonic, operands in prefix:
            require(not mnemonic.startswith(("call", "push", "pop")) and mnemonic not in ("syscall", "sysenter", "int"), "prehandoff call/stack/syscall")
            last = operands.rsplit(",", 1)[-1].strip()
            if not mnemonic.startswith(("cmp", "test", "j", "nop")):
                require(last not in ("%rsp", "%esp", "%sp", "%spl", "%rdi", "%edi", "%di", "%dil", "%rsi", "%esi", "%si", "%sil",
                                     "%rdx", "%edx", "%dx", "%dl", "%dh"), "opaque GPR/RSP write before handoff")
                require("(" not in last, "memory write before handoff")
        require(any("%r12" in row[2] and row[1].startswith("movabs") for row in prefix), "missing handoff marker")
    else:
        calls = [i for i, row in enumerate(body) if row[1].startswith("call")]
        require(calls and "lifecycle_raw_main" in body[calls[0]][2], "first entry call is not raw helper")
        prefix = body[:calls[0]]
        if role == "tiny-main":
            require(any(row[1].startswith("cmp") and row[2].endswith("%r12") for row in prefix), "missing incoming marker check")
            require(any(row[1].startswith("cmp") and "%r15" in row[2] for row in prefix), "missing incoming RSP check")
        for _, mnemonic, operands in prefix:
            require(mnemonic != "syscall", "syscall before completed startup checks")
            if not mnemonic.startswith(("cmp", "test", "j")):
                require(operands.rsplit(",", 1)[-1].strip() not in ("%r12", "%r12d", "%r12w", "%r12b"), "main initializes interpreter marker")
    return {"automatic_entry_disassembly_gate": "PASS", "requires_independent_source_and_disassembly_review": True,
            "runtime_startup_qualified": False}

def stack_gate(path):
    rows = []
    total = 208  # Raw assembly's fixed private Startup record.
    for line in path.read_text().splitlines():
        fields = line.split("\t")
        require(len(fields) == 3 and fields[1].isdigit() and fields[2] == "static", "unbounded or malformed compiler stack usage")
        amount = int(fields[1])
        total += amount
        rows.append({"function": fields[0], "frame_bytes": amount, "kind": fields[2]})
    require(rows and total <= 16384, "conservative sum of all raw stack frames exceeds16KiB")
    return {"rows": rows, "all_function_frames_plus_startup_bytes": total, "bound_bytes": 16384,
            "requires_source_review_of_nonrecursion": True}
def parent_elf_gate(path):
    data = path.read_bytes()
    require(64 <= len(data) <= MAX_COMMAND_FILE_BYTES, "hosted parent ELF size outside bound")
    header = struct.unpack_from("<16sHHIQQQIHHHHHH", data)
    require(header[0][:7] == b"\x7fELF\x02\x01\x01" and header[1:4] == (3, 62, 1), "parent is not a WOS x86_64 PIE")
    entry, phoff, phentsize, phnum = header[4], header[5], header[9], header[10]
    require(header[8] == 64 and phentsize == 56 and 1 <= phnum <= 32 and phoff + phnum * 56 <= len(data), "invalid parent PHDR table")
    programs = [struct.unpack_from("<IIQQQQQQ", data, phoff + i * 56) for i in range(phnum)]
    interpreters, stacks, dynamic, loads = [], [], [], []
    for program in programs:
        kind, flags, offset, address, _, filesz, memsz, _ = program
        require(offset <= len(data) and filesz <= len(data) - offset and filesz <= memsz, "parent program range outside ELF")
        if kind == 1:
            require(not (flags & 3 == 3), "parent has a writable executable load")
            loads.append(program)
        elif kind == 3:
            require(1 <= filesz <= 512, "parent interpreter string outside bound")
            interpreters.append(data[offset:offset + filesz])
        elif kind == 2:
            dynamic.append(program)
        elif kind == 0x6474E551:
            stacks.append(flags)
    require(interpreters == [b"/lib/ld.so\0"] and len(stacks) == 1 and not stacks[0] & 1, "parent interpreter/NX stack mismatch")
    require(any(p[1] & 1 and p[3] <= entry < p[3] + p[5] for p in loads), "parent entry is not executable file bytes")
    require(len(dynamic) == 1 and dynamic[0][5] % 16 == 0, "invalid parent dynamic table")
    rows = []
    for offset in range(dynamic[0][2], dynamic[0][2] + dynamic[0][5], 16):
        tag, value = struct.unpack_from("<qQ", data, offset)
        rows.append((tag, value))
        if tag == 0:
            break
    require(rows and rows[-1][0] == 0, "unterminated parent dynamic table")
    strings = [value for tag, value in rows if tag == 5]
    sizes = [value for tag, value in rows if tag == 10]
    require(len(strings) == len(sizes) == 1 and 0 < sizes[0] <= MAX_COMMAND_FILE_BYTES, "invalid parent dynamic strings")
    containing = [p for p in loads if p[3] <= strings[0] and strings[0] - p[3] <= p[5] and sizes[0] <= p[5] - (strings[0] - p[3])]
    require(len(containing) == 1, "parent strings outside one file-backed load")
    start = containing[0][2] + strings[0] - containing[0][3]
    table = data[start:start + sizes[0]]
    needed = []
    for tag, value in rows:
        if tag == 1:
            require(value < len(table) and table.find(b"\0", value) >= value, "invalid parent dependency string")
            needed.append(table[value:table.find(b"\0", value)].decode("ascii"))
    require(needed == ["libc.so"], "parent links a dependency beyond libc")
    return {"sha256": digest(data), "bytes": len(data), "elf_type": 3, "entry": entry, "phnum": phnum,
            "interpreter": "/lib/ld.so", "needed": needed, "structured_parent_elf_gate": "PASS"}

def allocated(path: Path) -> int:
    return sum(p.stat().st_blocks * 512 for p in path.rglob("*") if p.is_file())

def subreaper() -> None:
    if not sys.platform.startswith("linux"):
        raise ValueError("bounded owned-session runner requires Linux")
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")

def process_identity(pid: int) -> tuple[int, int, str, int, int]:
    fields = Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()
    return pid, int(fields[19]), fields[0], int(fields[1]), int(fields[3])

def owned_session(leader: int) -> list[tuple[int, int, str, int, int]]:
    result = []
    for path in Path("/proc").iterdir():
        if not path.name.isdecimal():
            continue
        try:
            identity = process_identity(int(path.name))
        except (FileNotFoundError, ProcessLookupError):
            continue
        # Sequential driver children only; the subreaper adopts escaped owned
        # compiler descendants. An original unreaped leader reserves the SID.
        if identity[4] == leader or identity[3] == os.getpid():
            result.append(identity)
    return result

def kill_identity(identity: tuple[int, int, str, int, int]) -> None:
    pid, started, state, _, _ = identity
    if state == "Z":
        return
    try:
        fd = os.pidfd_open(pid, 0)
    except ProcessLookupError:
        return
    try:
        if process_identity(pid)[1] == started:
            signal.pidfd_send_signal(fd, signal.SIGKILL)
    except (FileNotFoundError, ProcessLookupError):
        pass
    finally:
        os.close(fd)

def reap_group(process: subprocess.Popen, deadline: float) -> None:
    while True:
        identities = owned_session(process.pid)
        for identity in identities:
            kill_identity(identity)
        for pid, _, _, _, _ in identities:
            if pid == process.pid:
                continue
            try:
                os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                pass
        remaining = owned_session(process.pid)
        if all(pid == process.pid and state == "Z" for pid, _, state, _, _ in remaining):
            process.wait(timeout=max(0.01, deadline - time.monotonic()))
            if owned_session(process.pid):
                raise ValueError("owned session/adopted descendants remain after leader reap")
            return
        if time.monotonic() >= deadline:
            raise TimeoutError("owned session not proven absent; preserve scratch")
        time.sleep(0.01)

def command(argv: list[str], log: Path, scratch: Path, deadline: float, groups: list[int]) -> dict:
    if time.monotonic() >= deadline - 5:
        raise TimeoutError("60-second whole budget exhausted before command")

    def limits() -> None:
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        resource.setrlimit(resource.RLIMIT_FSIZE, (FILE_BUDGET, FILE_BUDGET))

    started = time.monotonic()
    process = None
    with log.open("xb") as output:
        try:
            process = subprocess.Popen(argv, stdout=output, stderr=subprocess.STDOUT,
                                       start_new_session=True, preexec_fn=limits,
                                       env=dict(os.environ, TMPDIR=str(scratch),
                                                ASAN_OPTIONS="detect_leaks=0:halt_on_error=1",
                                                LSAN_OPTIONS="detect_leaks=0"))
            groups.append(process.pid)
            while True:
                if log.stat().st_size > LOG_BUDGET or allocated(scratch) > SCRATCH_BUDGET:
                    raise ValueError("child log/private scratch exceeded bounded budget")
                if time.monotonic() >= deadline - 5:
                    raise TimeoutError("60-second whole budget exhausted")
                info = os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                if info is not None:
                    break
                time.sleep(0.02)
        finally:
            if process is not None:
                handlers = {sig: signal.signal(sig, signal.SIG_IGN)
                            for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
                try:
                    reap_group(process, deadline)
                    groups.remove(process.pid)
                finally:
                    for sig, handler in handlers.items():
                        signal.signal(sig, handler)
    if log.stat().st_size > LOG_BUDGET or allocated(scratch) > SCRATCH_BUDGET:
        raise ValueError("completed command exceeded storage/output budget")
    return {"argv": argv, "exit": process.returncode, "seconds": round(time.monotonic() - started, 6),
            "leader_reaped": True, "owned_session_absent": True, "log": pin(log)}

def main():
 global R,O
 parser=argparse.ArgumentParser(description=__doc__)
 parser.add_argument('--root',type=Path,required=True)
 parser.add_argument('--source-dir',type=Path,default=Path(__file__).resolve().parent)
 parser.add_argument('--output-dir',type=Path,required=True)
 parser.add_argument('--sysroot',type=Path,required=True)
 parser.add_argument('--llvm-bin',type=Path,required=True)
 parser.add_argument('--resource-dir',type=Path)
 parser.add_argument('--wire-header',type=Path)
 args=parser.parse_args()
 R=args.root.resolve(strict=True);S=args.source_dir.resolve(strict=True);O=args.output_dir.absolute();E=R/'build/goal39-evidence';RESULT=O/'result.json'
 require((R/'AGENTS.md').is_file(),'explicit WOS root')
 require(E.is_dir()and E.resolve()==E,'evidence parent must already exist without redirection')
 require(O.parent==E and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,95}',O.name),'fresh direct evidence child only')
 wire=(args.wire_header or R/'modules/kern/src/test/lifecycle_wire60.hpp').resolve(strict=True)
 sysroot=args.sysroot.resolve(strict=True);llvm=args.llvm_bin.resolve(strict=True)
 resource_dir=(args.resource_dir or llvm.parent/'lib/clang/22').resolve(strict=True)
 require(not os.path.lexists(O),'refuse pre-existing output directory')
 require(O.parent==E and E.resolve()==E and E.is_dir(),'exact evidence parent')
 require(shutil.disk_usage(E).free>=1<<30,'1GiB artifact free-space floor')
 started=time.monotonic();deadline=started+WHOLE;groups=[];scratch=None;identity=None;held={};generated={};selected=set();commands=[]
 result={'qualification':'FAIL','errors':[],'whole_budget_seconds':WHOLE,'source_held':False,'commands':commands,'runtime_qualified':False,'native_qualified':False,'criterion9_qualified':False,'full_goal_complete':False,'VM_actions':False,'current_artifact_writes':False}
 O.mkdir(mode=0o700)
 try:
  overlay=O/'include/test';overlay.mkdir(parents=True)
  private_header=overlay/'lifecycle_wire60.hpp';private_header.write_bytes(wire.read_bytes())
  private_source=O/'lifecycle-driver.cpp';original=(S/'compartment_lifecycle_driver.cpp').read_bytes()
  require(original.count(b'#include <test/lifecycle_wire60.hpp>')==1,'canonical shared-wire include')
  private_source.write_bytes(original);result['private_include_projection']={'source':pin(S/'compartment_lifecycle_driver.cpp'),'compiled_copy':pin(private_source),'byte_exact_copy':True,'header_source':pin(wire),'header_exact':private_header.read_bytes()==wire.read_bytes()}
  for p in (private_header,private_source):p.chmod(0o400)
  clang=llvm/'clang';clangxx=llvm/'clang++';linker=llvm/'ld.lld';readobj=llvm/'llvm-readobj';objdump=llvm/'llvm-objdump';resource=resource_dir
  explicit=[Path(__file__).resolve(),wire,private_header,private_source,R/'.vscode/tasks.json',R/'modules/compartment/CMakeLists.txt',R/'AGENTS.md',clang,clangxx,linker,readobj,objdump,Path(sys.executable),sysroot/'lib/ld.so',sysroot/'lib/libc.so']
  explicit.extend(S/n for n in ('compartment_lifecycle_driver.cpp','compartment_lifecycle_child.cpp','compartment_lifecycle_start.S','compartment_lifecycle_interp.S','validate_compartment_lifecycle_runtime.py','compartment_lifecycle_runtime.md'))
  candidates=set(explicit)
  for base in (R/'modules/kern/src/abi',sysroot/'include',sysroot/'lib',resource/'include',resource/'lib'):
   require(base.is_dir(),'missing actual target input directory '+str(base));candidates.update(p for p in base.rglob('*')if p.is_file())
  require(len(candidates)<=32768,'preheld input count exceeds bound')
  held={str(p):pin(p)for p in sorted(candidates)}
  require(held[str(wire)]['sha256']==held[str(private_header)]['sha256'] and held[str(wire)]['bytes']==held[str(private_header)]['bytes'],'compiled shared header differs from pinned source')
  require(held[str(S/'compartment_lifecycle_driver.cpp')]['sha256']==held[str(private_source)]['sha256'] and held[str(S/'compartment_lifecycle_driver.cpp')]['bytes']==held[str(private_source)]['bytes'],'compiled driver differs from pinned source')
  result['candidate_input_count']=len(held);result['candidate_input_census_sha256']=hashlib.sha256(json.dumps(held,sort_keys=True,separators=(',',':')).encode()).hexdigest()
  save(O/'candidate-inputs-before.json',list(held.values()))
  subreaper()
  scratch=Path(tempfile.mkdtemp(prefix='wos-lifecycle60-build01-',dir='/tmp'));st=scratch.stat();identity=(st.st_dev,st.st_ino,st.st_uid)
  require(st.st_uid==os.getuid()and not scratch.is_symlink(),'owned scratch identity')
  result['storage_before']={'free_bytes':shutil.disk_usage(E).free,'output':output_usage(),'scratch_budget_bytes':SCRATCH_BUDGET,'output_budget_bytes':OUTPUT_BUDGET}
  emitted=set();dep_rows=[]
  def run(label,argv,deps=()):
   output_usage();require(time.monotonic()<deadline-5,'whole artifact deadline exhausted')
   row={'name':label,'argv':list(map(str,argv))};commands.append(row)
   row.update(command(row['argv'],O/(label+'.log'),scratch,deadline,groups));require(row['exit']==0,'actual command failed '+label)
   for d in deps:
    inputs=[]
    for p in sorted(depfiles(d)):
     key=str(p)
     if key in held:require(pin(p)==held[key],'actual emitted input changed '+key);emitted.add(key);inputs.append(held[key])
     elif key in generated:require(pin(p)==generated[key],'owned linker input changed '+key);inputs.append(generated[key])
     else:raise ValueError('actual emitted external input not preheld '+key)
    dep_rows.append({'command':label,'depfile':pin(d),'inputs':inputs})
   output_usage()
  target=['--no-default-config','--target=x86_64-pc-wos',f'--sysroot={sysroot}']
  inc=['-nostdinc','-nostdinc++','-isystem',str(sysroot/'include/c++/v1'),'-isystem',str(resource/'include'),'-isystem',str(sysroot/'include'),'-I',str(O/'include'),'-I',str(R/'modules/kern/src')]
  driver_o=scratch/'driver.o';driver_d=O/'driver-compile.d'
  run('compile-driver',[clangxx,*target,'-resource-dir',resource,'-std=c++23','-O2','-g','-fPIE','-fno-stack-protector','-fno-stack-check','-Wall','-Wextra','-Werror','-fno-exceptions','-fno-rtti','-fno-sanitize=safe-stack',*inc,'-MD','-MF',driver_d,'-c',private_source,'-o',driver_o],[driver_d]);generated[str(driver_o)]=pin(driver_o)
  driver=O/'lifecycle-driver';driver_ld=O/'driver-link.d'
  run('link-driver',[clangxx,*target,'-resource-dir',resource,driver_o,f'-fuse-ld={linker}','-L'+str(sysroot/'lib'),'-nostdlib++','--rtlib=compiler-rt','--unwindlib=none','-lc','-Wl,-z,noexecstack','-Wl,--dynamic-linker=/lib/ld.so','-Wl,--dependency-file='+str(driver_ld),'-o',driver],[driver_ld])
  assembly=[clang,*target,'-c','-x','assembler-with-cpp']
  for label,source,mode in [('interpreter',S/'compartment_lifecycle_interp.S',None),('start-1',S/'compartment_lifecycle_start.S',1),('start-2',S/'compartment_lifecycle_start.S',2)]:
   obj=scratch/(label+'.o');d=O/(label+'.d')
   run('assemble-'+label,[*assembly,*([f'-DSTARTUP_MODE={mode}']if mode else[]),'-MD','-MF',d,source,'-o',obj],[d]);generated[str(obj)]=pin(obj)
  work=scratch/'work.o';work_d=O/'work-compile.d'
  run('compile-work',[clangxx,*target,'-std=c++23','-O2','-ffreestanding','-fno-builtin','-fno-pic','-fno-pie','-fno-exceptions','-fno-rtti','-fno-stack-protector','-fno-stack-check','-fno-unwind-tables','-fno-asynchronous-unwind-tables','-fno-sanitize=safe-stack','-mno-red-zone','-mno-mmx','-mno-sse','-mno-sse2','-fno-vectorize','-fno-slp-vectorize','-fstack-usage','-Wall','-Wextra','-Werror',*inc,'-MD','-MF',work_d,'-c',S/'compartment_lifecycle_child.cpp','-o',work],[work_d]);generated[str(work)]=pin(work)
  stack=scratch/'work.su';result['raw_conservative_stack_gate']=stack_gate(stack);shutil.copyfile(stack,O/'work.su')
  common=[linker,'-m','elf_x86_64','--no-undefined','-e','_start','-z','separate-code','-z','max-page-size=4096','-z','noexecstack','--build-id=none']
  interp=O/'lifecycle-tiny-ld.so';d=O/'interpreter-link.d'
  run('link-interpreter',[*common,'-shared','--no-dynamic-linker',scratch/'interpreter.o','--dependency-file='+str(d),'-o',interp],[d])
  static=O/'lifecycle-raw-static';tiny=O/'lifecycle-raw-tiny'
  for mode,out,opt in [(1,static,'--no-dynamic-linker'),(2,tiny,'--dynamic-linker=/lib/lifecycle-tiny-ld.so')]:
   d=O/f'main-{mode}-link.d';run(f'link-main-{mode}',[*common,'--image-base=0x400000',opt,scratch/f'start-{mode}.o',work,'--dependency-file='+str(d),'-o',out],[d])
  rows=[]
  for path,role in [(driver,'driver'),(static,'static-main'),(tiny,'tiny-main'),(interp,'interpreter')]:
   metadata=parent_elf_gate(path)if role=='driver'else elf_gate(path,role)
   run('inspect-'+role,[readobj,'--file-headers','--program-headers','--dynamic-table','--relocations','--symbols',path])
   run('disassemble-'+role,[objdump,'-d','--no-show-raw-insn',path])
   if role!='driver':metadata['entry_disassembly']=entry_disassembly_gate(O/('disassemble-'+role+'.log'),metadata,role)
   path.chmod(0o555);rows.append({'role':role,'artifact':pin(path),'elf':metadata})
  selected={str(p)for p in explicit}|emitted
  result['source_before']=[held[p]for p in sorted(selected)];result['source_after']=[pin(Path(p))for p in sorted(selected)]
  result['source_held']=result['source_before']==result['source_after'];require(result['source_held'],'actual source/dependency closure changed')
  result['emitted_dependency_files']=dep_rows;result['actual_emitted_external_dependency_count']=len(emitted);result['owned_intermediate_inputs']=list(generated.values());result['artifacts']=rows
  # Four completed genuine ELFs, metadata/dep gates and source holds precede manifest generation.
  nonce=0
  while nonce==0:nonce=secrets.randbits(64)
  data=bytearray(1024);struct.pack_into('<8Q',data,0,0x43394d414e494645,1,1024,4,nonce,8,24,1000)
  for i,row in enumerate(rows):
   meta=row['elf'];p=row['artifact'];off=64+128*i
   struct.pack_into('<5Q',data,off,i+1,p['bytes'],meta['elf_type'],meta['entry'],meta['phnum']);data[off+40:off+72]=bytes.fromhex(p['sha256'])
  manifest=O/'wos-lifecycle60-manifest.bin';manifest.write_bytes(data);manifest.chmod(0o444);result['manifest']={'artifact':pin(manifest),'nonce':nonce,'generated_from_actual_ELFs_only':True,'reserved_zero':not any(data[576:]),'guest_root_UID_GID_ownership_not_yet_installed':True}
  require(result['source_after']==[pin(Path(p))for p in sorted(selected)],'final post-manifest source hold')
  result['qualification']='PASS_ACTUAL_LIFECYCLE60_FOUR_ELF_AND_MANIFEST_ONLY'
 except BaseException as error:result['errors'].append(type(error).__name__+': '+str(error))
 finally:
  result['owned_groups_pending']=groups.copy()
  result['scratch_removed']=scratch is None
  try:
   if scratch is not None:
    st=scratch.stat();exact=(st.st_dev,st.st_ino,st.st_uid)==identity and not scratch.is_symlink();result['scratch_allocated_before_cleanup']=allocated(scratch)
    if exact and not groups:shutil.rmtree(scratch)
    result['scratch_removed']=not scratch.exists()
    if not result['scratch_removed']:result['errors'].append('exact owned scratch cleanup unproved')
  except BaseException as error:result['errors'].append('cleanup '+type(error).__name__+': '+str(error))
  result['elapsed_seconds']=time.monotonic()-started
  try:result['storage_after']={'free_bytes':shutil.disk_usage(E).free,'output':output_usage()}
  except BaseException as error:result['errors'].append('storage '+type(error).__name__+': '+str(error))
  if result['elapsed_seconds']>=WHOLE:result['errors'].append('whole120s budget exceeded')
  if result['errors']:result['qualification']='FAIL'
  save(RESULT,result);print(json.dumps({'result':pin(RESULT),'qualification':result['qualification'],'errors':result['errors'],'elapsed_seconds':result['elapsed_seconds'],'scratch_removed':result['scratch_removed']}))
 return 0 if not result['errors']else 1

if __name__=='__main__':raise SystemExit(main())
