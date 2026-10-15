# Genuine tiny WOS interpreter and raw lifecycle artifacts

Historical Goal 39 prototype. The builder requires ignored checkpoint 41
admissions and source/tool receipts from the original workspace; it is not a
standalone fresh-checkout build command. The frozen protocol and source are
preserved for reference. Use [the maintained lifecycle fixture](compartment_lifecycle_runtime.md)
for current lifecycle qualification. The milestone descriptions below record
the original prototype's scope, not current whole-goal acceptance.

These five new fixture files implement the artifact-only milestone from
`goals/progress/39/contract-41-tiny-interpreter-artifacts.md`. The narrow approved
PHDR correction is in `supplement-41-tiny-artifact-phdr-correction.md`; historical
contract/capture bytes remain unchanged. No runtime controller or hosted parent is
implemented here. Compilation/ELF inspection cannot qualify private startup,
fault consumption, Task/File/namespace closure, 1000 cycles or24 injected failures.

The tiny interpreter is a genuine relocation-free ET_DYN startup adapter for the
exact fully resolved raw main. It is not a general dynamic linker. Installed
ordinary `ld.so` remains untouched and is too large for INTERPRETER_BUFFER.

## Files and artifact roles

- `lifecycle_tiny_interp.S`: hidden `_start`, no successful-path call/syscall/
  allocation/stack or data write. Preserves RSP and opaque RDI/RSI/RDX and bounds
  native stack parsing. It hands off by the actual AT_ENTRY, with marker in R12,
  actual interpreter entry in R13, AT_BASE in R14 and original RSP in R15.
- `lifecycle_raw_start.S`: separately compiled `STARTUP_MODE=1/2`; checks genuine
  startup before its first helper call. The208-byte,26-word private Startup record
  captures the real RSP/auxv/opaque GPRs. Static mode never manufactures a tiny
  interpreter marker.
- `lifecycle_raw_work.cpp`: actual raw WOS calls, all256 initial FD slots and all16
  admitted limits, credentials/no-new-privileges/authority/network, real bounded
  File/SysV work, immutable READY and fresh continuing work heartbeat. No fork,
  pthread, private mount, libc, CRT, TLS, global constructor or successful exit.
  Local bounded `memcpy`/`memset` definitions support freestanding compiler
  lowering without adding a runtime library.
- `build_lifecycle_raw_artifacts.py`: fresh output ownership, full source/tool/
  target-include pins, explicit target include search, bounded compiler commands,
  structured ELF gates plus metadata/disassembly receipts. It never launches WOS.
- This README specifies the proposed hosted-parent protocol. That parent and all
  driver/controller/census integration remain separate work.

The outputs are `lifecycle-raw-static` (variant1, ET_EXEC/no PT_INTERP),
`lifecycle-raw-tiny` (variant2, ET_EXEC/real PT_INTERP
`/lib/lifecycle-tiny-ld.so`) and `lifecycle-tiny-ld.so` (ET_DYN/no PT_INTERP).
All final files must be1..65536 bytes, no runtime relocations/dependencies/TLS/
IFUNC/constructors/RWX, real NX GNU_STACK and valid bounded load/metadata layout.
The main is linked at0x400000 and loads within0x400000..0x800000. The adapter uses
the actual randomized2MiB-aligned interpreter bias0x30000000..0x70000000.

## Source-only versus actual gates

After root's source review, root may run the helper from the repository root:

```sh
python3 tests/fixtures/build_lifecycle_raw_artifacts.py --output-dir build/goal39-evidence/lifecycle-raw41-artifacts01
```

The output must be a new direct child of `build/goal39-evidence`; an existing
directory/file/symlink is rejected. It preserves failed small output evidence,
uses no VM disks and caps command duration30s, child file size1MiB and total
artifact directory8MiB. It records exact argv/cwd/minimal environment and full
five-source bytes, verifies the immutable pre-edit51-source/tool baseline and
target header tree before/after. Clang default configs and Host include discovery
are disabled. Explicit WOS sysroot libc++, actual Clang22 resource headers and
WOS sysroot C headers are the sole system include paths; kernel ABI headers use
the explicit source path. No sysroot/kernel/build-cache mutation occurs.

`source-before.json` and `artifact-receipt.json` preserve small provenance,
command logs, structured ELF symbols/programs/dynamic gates and entry disassembly.
Compiler stack-usage metadata must have only static frames; summing every emitted
raw function frame plus the208-byte startup record must stay<=16KiB. That
conservative gate also needs independent source review proving no recursion.
Result `PASS_ARTIFACT_ONLY` means compiler/ELF/disassembly validation only.
Independent source/disassembly review remains mandatory. A future real nominal
private CREATE warmup must prove actual startup, work, positive-PID kill/wait;
later scoped fault and census gates separately prove consumption/retirement.
Every script receipt sets runtime/private-startup/stage/census qualificationfalse.

## Genuine startup and current copied header shape

RSP is authoritative; direct user entry and saved-context iret can supply different
opaque RDI/RSI/RDX values. The tiny adapter preserves those three and RSP. Marker
R12 is `0x54494e593431484f`; raw tiny entry checks it and RSP==R15 immediately,
then AT_BASE==R14 and BASE+actual ELF e_entry==R13 before any helper/syscall.
Static mode requires AT_BASE absent and records incoming R12 as opaque.

Current native argv is exactly one string `/app/lifecycle-raw` (18bytes), envc0.
Auxv nonnull pairs: static12/tiny13. IDs11/12/13/14 are100, SECURE23 is0, PAGESZ6
is4096, ENTRY9 is the main entry, PHENT4 is56, PHNUM5 is1..32, RANDOM25/EXECFN31
are real user pointers and BASE7 exists only for tiny. Main metadata is checked
against the real image and the actual readonly copied headers. ET_EXEC copies
Ehdr64 at0x1000 and AT_PHDR at0x1040; original PT_PHDR vaddr/paddr alone are
rewritten to0x1040. Complete copy ends<=0x2000. This is a value comparison, not
a userspace PTE-permission experiment. The host gate requires real original ELF/
PHDR bytes in a nonexecuting readable first load at0x400000, e_phoff64.

Private Startup26 words:0RSP,1own_start,2ENTRY,3BASE,4interp_start,5incomingR12,
6RDI,7RSI,8RDX,9argc,10envc,11PHDR,12PHENT,13PHNUM,14PAGESZ,15SECURE,
16UID,17EUID,18GID,19EGID,20RANDOM,21EXECFN,22nonnullAux,23startupFlags,
24interp_e_entry,25STARTUP_MODE. Static BASE/interp_start/interp_e_entry are0.
Flags bits0..6 are STACK,MAIN_ENTRY,UID_AUX,GID_AUX,SECURE,PAGESZ,PH_SHAPE;
bit7 is genuine TINY_HANDOFF. Require static0x7f/tiny0xff.

## Proposed per-wave parent ownership and input

The future Host/UID100 parent creates a fresh `wave-COOKIE.raw` with actual
O_CREAT|O_EXCL|O_RDWR|O_CLOEXEC0600 regular File, truncates8192bytes and captures
Stat ino/dev plus F_GETFL. It writes the input/commit, fsyncs and reads it back
before CREATE. The actual returned positive Host PID, globally nonreused
cookie1..2^60-1 and nonce>0 identify this wave; no old receipt adoption.
Spec uses LOCAL/networkDENIED, authority0/servicepriv0, requested finite permitted
policy, real regular assets mode0755 at `/app/lifecycle-raw` and optional
`/lib/lifecycle-tiny-ld.so`, argv1/env0 and one destinationFD9 grant. No ambient
stdio is inherited. Root+real populator only; raw workload never forks/threads.

FD9 retains the same real File/open rights; child's descriptor CLOEXEC isfalse
by actual native FdReservation default. All input/work/report IO uses explicit
PREAD/PWRITE offsets. Child validates FD9 regular/8192/ino/dev/F_GETFL and initial
F_GETFD0; every other FD0..255 must give-EBADF=-9 before any local open.

File layout: immutable512-byte input at0, continuing256-byte heartbeat at1024,
immutable2048-byte READY at4096, other initial bytes0, total8192. Every scalar
is explicit LE64; signed raw errno results are two's-complement int64 bits.
No C++ padding is serialized. Input is64 words:

| Words | Exact value |
| --- | --- |
| 0..5 | magic0x4131524157494e50,version1,bytes512,nonce,cookie,variant1/2 |
| 6..13 | actualHostParentPID>2,start_us,deadline=start+10000000,UID100,NNP1,authority0,service256,network0 |
| 14..15 | reserved0 |
| 16..47 |16 exact admittedSpec soft/hard pairs; <=actual parent/selectedservice ceilings, soft<=hard |
| 48..55 | READYoffset4096,READYbytes2048,Filebytes8192,workbytes32,SysVkey0x4141,SysVsize4096,argc1,envc0 |
| 56..63 | FD9ino,dev,parentF_GETFL,heartbeatOffset1024,heartbeatBytes256,initialRounds16,maxRounds10000,commitNonce |

Parent writes whole input with word63zero then commits nonce at byte504, fsyncs;
child requires two identical512-byte reads and nonce==commit. The input is never
changed. NOFILEsoft/hard>=10, FSIZEsoft/hard>=8192, sufficient actual AS/STACK/
DATA/root/NPROC/CPU and service ceilings are prerequisites, not locally raised
limits. Actual raw GETRLIMIT0..15 pairs must equal accepted input/Spec exactly.
Credential observations are PID1/PPID0, UID/EUID/GID/EGID100, NNP1, authority0,
locked service result256. Valid IPv4/IPv6 SOCK_STREAM/protocol0 calls both must
return-EPERM=-1. SECURE0 is consistent with NNP1/equal IDs/no service grants.

## Real live work and READY report

The admitted private TMPFS root belongs to UID/GID100. Raw OPEN creates exactly
one `/live.bin` O_CREAT|O_EXCL|O_RDWR0600; one SysV key0x4141/4096bytes/0600 is
created and attached once. Each round writes/PREADs exact32 File bytes
`{nonce,cookie,round,~round}`, writes/reads the same four volatile shared words,
looks up the same SysV key and obtains real IPC_STAT. Check every signed result,
payload and key/size/ownership/mode/nattch1. Keep workFD, segment/attachment and
FD9 live until parent SIGKILL; no close, detach or RMID in a successful run.
Current SysV cpid/lpid metadata is physical; no localPID1 assertion conceals the
separate namespace-PID visibility followup. This is single-process object work,
not writable MAP_SHARED coherence, intertree futex or comprehensive leak proof.

After16 successful rounds, private File FSYNC (actual TMPFS no-op0), FSTAT size32/
regular0600/UID/GID100 and checked clock, publish READY once. Write2048bytes with
word255zero then commit nonce at byte6136; READY never changes afterward. Parent
requires two identical complete reads, every oracle below, then parent FSYNC and
exact readback before kill. A later workload error still fails via consuming wait.

READY256 words:

| Words | Exact meaning / qualification requirement |
| --- | --- |
| 0..7 | magic0x4131524157525054,version1,bytes2048,READYstage1,nonce,cookie,variant,MODE==variant |
| 8..16 | PID1,PPID0,UID100,EUID100,GID100,EGID100,NNP1,authority0,service256 |
| 17..21 | startupflags0x7f/0xff,failures0,first0,signedlastFailure0,checks>0 |
| 22..31 | parentPID,start_us,rawStart_us,READY_us,deadline,rounds16,absentFDs255,FD9flags,ino,dev |
| 32..47 |16 raw GETRLIMIT results0 |
| 48..56 | workFD0..255!=9,workF_GETFL lowbits2,workFSTAT0,positive int32shmid,pagealigned userattachment,same-key lookup==shmid,IPC_STAT0,IPv4socket-1,IPv6socket-1 |
| 57..63 | payloadRound16,~16,workFileino,dev,size32,modeS_IFREG|0600,UID100 |
| 64..95 |16 observed soft/hard pairs equal input16..47 |
| 96..111 | originalRSP,own_start,ENTRY,BASE,interp_start,incomingR12,opaqueRDI/RSI/RDX,argc1,envc0,PHDR0x1040,PHENT56,PHNUM1..32,PAGESZ4096,SECURE0 |
| 112..121 | FD9size8192,FSTAT0,inputPREAD512,workGID100,SysVkey0x4141,size4096,UID100,GID100,mode0600,nattch1 |
| 122..127 | Filepayload nonce,cookie,16,~16; SysVpayload nonce,cookie |
| 128..143 |16 signed private PWRITE results32 |
| 144..159 |16 signed private PREAD results32 |
| 160..175 |16 same-key SHMGET results exactshmid |
| 176..191 |16 signed IPC_STAT results0 |
| 192..207 |16 File comparisons1 |
| 208..223 |16 SysV comparisons1 |
| 224..239 | inputcommitnonce,privateFSYNC0,onlyFD9proof1,AT_UID/EUID/GID/EGID100,twoInputReads1,mainEntry+copiedHeadersChecked1,interpHeaderChecked(tiny1/static0),RANDOMptr,EXECFNptr,nonnullAux13/12,interp_e_entry(tinypositive/static0),READYclock0,initialClock0 |
| 240..254 | reserved0 |
| 255 | commitNonce==word4 |

Parent independently compares entry/PHNUM with pinned artifact metadata, tiny
BASE with actual aligned biaswindow and interp_start==BASE+pinnedinterp_e_entry,
marker exact and RSP shape. Static BASE/interpreter fields0; incomingR12 opaque.
Check parentstart<=rawstart<=READY<deadline and every signed result. PASS text
alone has no role. Partial/error reports remain evidence of failure.

After READY, actual work rounds17..10000 continue. Each successful round publishes
a fresh32-word heartbeat256bytes at1024 (full commit0 then round commit at1272):
0magic0x41314c4956454842,1version1,2nonce,3cookie,4variant,5round17..10000,
6monotonic_us,7PWRITE32,8PREAD32,9lookupshmid,10STAT0,11round,12~round,
13checks>0,14failures0,15first0,16workFD,17shmid,18attachmentVA,19reserved0,
20Filematch1,21SysVmatch1,22UID100,23PID1,24..30reserved0,31commitSequence==round.
Parent requires two identical reads, a fresh matching round, READY object/attempt
identity and time>=READY. This shows real continued work, not internal lock/SMP
overlap or GC progress. Child then actually sleeps1ms, checks monotonic/deadline
and repeats. Short IO/EINTR/unexpected negative results are failure, no unbounded
retry. All scans/rounds are bounded and absolute deadline is10s after parentstart.

Future parent observes READY within5s after CREATE return and a post-READY fresh
heartbeat>=17, then kills EXACT positive returned Host PID with SIGKILL9 within2s
of that observation and before deadline-1s. No0/-1/group or discovery signal.
Close parent reportFD/drop extra observer before exact consuming wait; require
same positive PID/raw status9. Mutable File/Task/namespace closure is a later
controller/census gate. Failed CREATE's transient reportFD needs explicit close
boundary/accounting before baseline equality; that integration is unimplemented.

Interpreter failure exits100(stack)/101(count)/102(auxv)/103(header). Main/raw
failures104startup,105identity/limits,106input/grant,107File,108SysV,109network,
110clock/deadline,111report,112sleep,113shape,114unexpectedhelperreturn. Raw success
never exits0 or self-kills. Actual positive kill/wait and feasible pilot cadence
remain unqualified until root runs the approved warmup.

No VM is started by the builder. Future runtime preserves authorized rootless
`--no-setup`, one topology, private COW/reflink, measured storage bounds and
exact-owned stop/prune on PASS and FAIL. Keep small receipts/current artifact
seeds; retain no completed full VM disks. Uninjected warmups are separate from
counted1000success/24failure and precede the real zero-private census baseline.
